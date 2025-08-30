# datacenter.py
# DataCenter that ingests an Azure VM trace CSV and simulates hourly demand for one week,
# with MICRO-FOUNDATION job-level scheduling for two strategies:
#   - only_curtail (best-effort: schedule jobs only when curtailed IT power is available)
#   - carbon_responder (postpone SHIFTABLE jobs within per-job slack to low price/carbon hours)
# If neither is enabled, jobs run as-is (original timestamps).

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Sequence, List, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from battery import Battery  # Local import
from concurrent.futures import ThreadPoolExecutor
import threading

# ------------------------------
# Strategy config (job-level)
# ------------------------------
@dataclass
class CarbonResponderConfig:
    # Max postponement window (hours) per shiftable job from its original release time.
    max_shift_hours: int = 24
    # weights is the weight for optimization, CarbonResponder optimizes the combination of
    # job total waiting time and energy costs and carbon emissions.
    weights: dict[str, float] = field(default_factory=lambda: {
        "waiting_time": 1.0,
        "energy_cost": 1.0,
        "carbon_emissions": 1.0
    })

# ------------------------------
# Core config
# ------------------------------
@dataclass
class DataCenterConfig:
    capacity_mw: float = 20.0                # IT capacity (MW)
    pue: float = 1.2                         # Power Usage Effectiveness
    watts_per_vcpu: float = 20.0             # W@100% util per vCPU (IT power model coeff)
    default_vcpu: int = 2                    # Fallback if bucket missing/invalid
    utilization_column: str = "avg_cpu"      # 'avg_cpu' or 'p95 max cpu'
    week_start: datetime = datetime(2025, 1, 6)
    week_hours: int = 7 * 24
    timezone: str = "America/Los_Angeles"    # epoch alignment

# ------------------------------
# Job model
# ------------------------------
@dataclass
class VMJob:
    # Times are integer hour indices relative to the simulated week window.
    release_h: int          # earliest hour we can start (inclusive)
    end_orig_h: int         # original planned end (exclusive), used for default schedule
    duration_h: int         # duration in hours (ceil of overlap hours)
    it_power_mw: float      # IT power consumption in MW
    shiftable: bool         # whether this job can be postponed
    job_id: int             # identifier (index in CSV)

# ------------------------------
# DataCenter with job-level strategies
# ------------------------------
@dataclass
class DataCenter:
    csv_path: str
    config: DataCenterConfig = field(default_factory=DataCenterConfig)
    battery: Optional[Battery] = None
    scale_jobs: bool = True
    _hourly_it_mw_raw: Optional[np.ndarray] = field(default=None, init=False, repr=False)  # raw from trace (no scaling)
    _jobs: Optional[List[VMJob]] = field(default=None, init=False, repr=False)

    # ---------- CSV ingestion ----------
    def _parse_csv(self) -> Tuple[pd.DataFrame, str, str]:
        # Handle vmtable.csv format with predefined headers
        column_headers = [
            'vm id', 'subscription id', 'deployment id', 'timestamp vm created', 'timestamp vm deleted',
            'max cpu', 'avg cpu', 'p95 max cpu', 'vm category', 'vm virtual core count bucket', 'vm memory (gb) bucket'
        ]
        df = pd.read_csv(self.csv_path, header=None)
        df.columns = column_headers
        df.columns = [c.strip().lower() for c in df.columns]
        
        required = ["vm id", "timestamp vm created", "timestamp vm deleted"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        util_col = self.config.utilization_column.lower()
        if util_col not in df.columns:
            alt = "avg cpu" if "avg cpu" in df.columns else None
            if alt is None:
                raise ValueError(f"Utilization column '{util_col}' not found and no 'avg cpu' available.")
            util_col = alt

        vcpu_col = "vm virtual core count bucket"
        if vcpu_col not in df.columns:
            df[vcpu_col] = self.config.default_vcpu

        for col in [util_col, vcpu_col, "timestamp vm created", "timestamp vm deleted"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=[util_col, vcpu_col, "timestamp vm created"])
        df["timestamp vm deleted"] = df["timestamp vm deleted"].fillna(df["timestamp vm created"] + 3600)

        df = df[df[util_col].between(0, 100)]
        df = df[df[vcpu_col] > 0]
        return df, util_col, vcpu_col

    # ---------- Jobs extraction (micro foundation) ----------
    def _extract_jobs_from_vms(self) -> List[VMJob]:
        df, util_col, vcpu_col = self._parse_csv()
        H = self.config.week_hours
        week_start_epoch = pd.Timestamp(self.config.week_start, tz=self.config.timezone).timestamp()
        week_end_epoch = week_start_epoch + H * 3600

        jobs: List[VMJob] = []
        watts_per_vcpu = self.config.watts_per_vcpu

        for idx, row in df.iterrows():
            created = float(row["timestamp vm created"])
            deleted = float(row["timestamp vm deleted"])
            if deleted <= created:
                deleted = created + 3600.0

            # overlap with week window
            vm_s = max(created, week_start_epoch)
            vm_e = min(deleted, week_end_epoch)
            if vm_e <= vm_s:
                continue

            s_idx = int((vm_s - week_start_epoch) // 3600)
            e_idx = int(np.ceil((vm_e - week_start_epoch) / 3600.0))
            s_idx = max(0, s_idx)
            e_idx = min(H, e_idx)

            duration_h = max(e_idx - s_idx, 1)
            util = float(row[util_col]) / 100.0
            vcpus = int(row[vcpu_col]) if not np.isnan(row[vcpu_col]) else self.config.default_vcpu

            it_power_mw = (watts_per_vcpu * vcpus * util) / 1e6
            if it_power_mw <= 0.0:
                continue

            # Mark some jobs as shiftable later based on energy share; here default: all candidates shiftable;
            # we'll down-select by energy fraction in the carbon_responder function.
            jobs.append(VMJob(
                release_h=s_idx,
                end_orig_h=e_idx,
                duration_h=duration_h,
                it_power_mw=it_power_mw,
                shiftable=True,
                job_id=idx
            ))

        self._jobs = jobs
        return jobs

    # ---------- Build raw IT load from jobs (no scaling) ----------
    def _build_hourly_it_from_jobs_default(self, jobs: List[VMJob]) -> np.ndarray:
        """Build hourly IT load from jobs using original timestamps"""
        H = self.config.week_hours
        it = np.zeros(H, dtype=float)
        
        # Fully vectorized using numpy
        if jobs:
            # Extract job data as arrays
            starts = np.array([max(0, j.release_h) for j in jobs])
            ends = np.array([min(H, j.end_orig_h) for j in jobs])
            powers = np.array([j.it_power_mw for j in jobs])
            
            # Create indices and values for all hours
            indices = []
            values = []
            for i, (start, end, power) in enumerate(zip(starts, ends, powers)):
                if end > start:
                    indices.extend(range(start, end))
                    values.extend([power] * (end - start))
            
            # Vectorized accumulation
            if indices:
                np.add.at(it, indices, values)
        
        return it

    # ---------- Scaling ----------
    def _get_scaled_jobs(self) -> List[VMJob]:
        """Scale job power by multiplicative factor or sampling"""
        if self._jobs is None:
            self._extract_jobs_from_vms()
        if self._hourly_it_mw_raw is None:
            self._hourly_it_mw_raw = self._build_hourly_it_from_jobs_default(self._jobs)
        
        current_peak_it_mw = self._hourly_it_mw_raw.max()
        target_it_mw = self.config.capacity_mw  # This is already IT capacity
        
        if current_peak_it_mw <= 0:
            return self._jobs
        
        scale_factor = target_it_mw / current_peak_it_mw
        
        if scale_factor >= 1.0:
            # upscale: multiply each job's power by scale_factor
            return [VMJob(
                release_h=j.release_h, end_orig_h=j.end_orig_h, duration_h=j.duration_h,
                it_power_mw=j.it_power_mw * scale_factor, shiftable=j.shiftable, job_id=j.job_id
            ) for j in self._jobs]
        else:
            # downscale: sample without replacement
            n_samples = int(len(self._jobs) * scale_factor)
            job_df = pd.DataFrame([{
                'release_h': j.release_h, 'end_orig_h': j.end_orig_h, 'duration_h': j.duration_h,
                'it_power_mw': j.it_power_mw, 'shiftable': j.shiftable, 'job_id': j.job_id
            } for j in self._jobs])
            sampled = job_df.sample(n=n_samples, replace=False, random_state=42)
            
            return [VMJob(
                release_h=int(row['release_h']), end_orig_h=int(row['end_orig_h']), 
                duration_h=int(row['duration_h']), it_power_mw=float(row['it_power_mw']),
                shiftable=bool(row['shiftable']), job_id=int(row['job_id'])
            ) for _, row in sampled.iterrows()]

    # ---------- Job-level scheduling helpers ----------
    def _get_day_boundaries(self, curtailed_facility_mw: np.ndarray, strategy: str) -> List[Tuple[int, int]]:
        """Get day boundaries based on strategy"""
        boundaries = []
        curtail_length = []

        if strategy == "curtail_only":
            # Find all curtailment windows
            prev_curtail_end = 0  # Start from week beginning
            
            for day in range(7):
                day_start = day * 24
                day_end = min(day_start + 24, len(curtailed_facility_mw))
                
                # Find curtailment in this day
                curtail_found = False
                for h in range(day_start, day_end):
                    if h < len(curtailed_facility_mw) and float(curtailed_facility_mw[h]) > 0:
                        # Find end of this curtailment
                        curtail_start = h  # This is the start of curtailment
                        curtail_end = h
                        while (curtail_end < len(curtailed_facility_mw) and 
                               curtail_end < len(curtailed_facility_mw) and
                               float(curtailed_facility_mw[curtail_end]) > 0):
                            curtail_end += 1
                        
                        # Boundary: from previous curtail end to current curtail end
                        boundaries.append((prev_curtail_end, curtail_end))
                        prev_curtail_end = curtail_end
                        curtail_found = True
                        curtail_length.append(curtail_end - curtail_start)
                        break
                
                # If no curtailment found in this day, still create boundary to day end
                if not curtail_found:
                    # boundaries.append((prev_curtail_end, day_end))
                    # prev_curtail_end = day_end
                    raise ValueError("No curtailment found for day")
        else:
            raise ValueError(f"Unknown strategy: {strategy}")
        return boundaries, curtail_length

    def _schedule_day_jobs(self, args) -> Tuple[np.ndarray, int]:
        """Schedule jobs for a single day"""
        day_jobs, day_start, day_end, cap_it_by_curtail, strategy, day_price, curtail_length = args
        
        day_length = day_end - day_start
        used = np.zeros(day_length, dtype=float)
        scheduled_count = 0
        use_battery = "_battery" in strategy
        base_strategy = strategy.replace("curtail_battery", "curtail_only").replace("_battery", "")
        
        # Handle both as_is and curtail_only strategies
        if base_strategy == "as_is":
            # As-is scheduling within day boundaries
            for job in day_jobs:
                job_start = max(0, job.release_h - day_start)
                job_end = min(day_length, job.end_orig_h - day_start)
                if job_end > job_start:
                    for h in range(job_start, job_end):
                        used[h] += job.it_power_mw
                    scheduled_count += 1
            return used, scheduled_count

        # first, for job length > curtailment window length we can drop them for now
        day_jobs = [j for j in day_jobs if j.duration_h <= curtail_length]

        # Simple approach: schedule jobs only when curtailment is available
        for job in day_jobs:
            job_rel_h = job.release_h - day_start
            
            # Try to schedule job starting from its release time or later
            for start_h in range(max(0, job_rel_h), day_length - job.duration_h + 1):
                abs_start = day_start + start_h
                
                # Check if all hours have curtailment capacity
                can_schedule = True
                for check_h in range(job.duration_h):
                    abs_check_h = abs_start + check_h
                    rel_check_h = start_h + check_h
                    
                    if rel_check_h >= day_length:
                        can_schedule = False
                        break
                        
                    curtail_cap = float(cap_it_by_curtail[abs_check_h]) if abs_check_h < len(cap_it_by_curtail) else 0.0
                    
                    # Must have curtailment capacity
                    if curtail_cap <= 0 or (curtail_cap - used[rel_check_h]) < job.it_power_mw:
                        can_schedule = False
                        break
                
                if can_schedule:
                    # Schedule the job
                    for place_h in range(job.duration_h):
                        used[start_h + place_h] += job.it_power_mw
                    scheduled_count += 1
                    break  # Job scheduled, move to next job
    
        return used, scheduled_count

    def _schedule_parallel(self, jobs: List[VMJob], curtailed_facility_mw: np.ndarray, strategy: str, 
                          price_vector: Optional[Sequence[float]] = None) -> np.ndarray:
        """Parallel scheduling across days"""
        H = self.config.week_hours
        cap_it_by_curtail = curtailed_facility_mw / self.config.pue
        used = np.zeros(H, dtype=float)
        
        # Get day boundaries
        base_strategy = strategy.replace("curtail_battery", "curtail_only").replace("_battery", "")
        boundaries, curtail_lengths = self._get_day_boundaries(curtailed_facility_mw, base_strategy)

        # # sanity check: to debug the problem we make all jobs 1hr long
        # for j in jobs:
        #     j.duration_h = 1

        # for curtailment only, jobs that length >= 20hr can be dropped now
        jobs = [j for j in jobs if j.duration_h < 20]

        # Prepare arguments for each day
        day_args = []
        for day, (day_start, day_end) in enumerate(boundaries):
            # Get jobs for this day
            if base_strategy == "as_is":
                day_jobs = [j for j in jobs if day_start <= j.release_h < day_end]
            else:  # curtail_only
                day_jobs = [j for j in jobs if day_start <= j.release_h < day_end]
            
            day_price = price_vector[day_start:day_end] if price_vector is not None else None
            curtail_length = curtail_lengths[day] if day < len(curtail_lengths) else 0
            day_args.append((day_jobs, day_start, day_end, cap_it_by_curtail, strategy, day_price, curtail_length))
        
        # Parallel execution with progress bar
        total_scheduled = 0
        print(f"[INFO] Scheduling {len(day_args)} days in parallel...")
        
        with ThreadPoolExecutor(max_workers=7) as executor:
            futures = [executor.submit(self._schedule_day_jobs, args) for args in day_args]
            results = []
            
            for i, future in enumerate(futures):
                result = future.result()
                results.append(result)
                progress = (i + 1) / len(futures) * 100
                print(f"\r[PROGRESS] Day {i+1}/7 completed ({progress:.1f}%)", end="", flush=True)
            
        print()  # New line after progress bar
        
        # Combine results
        for day, (day_used, day_scheduled) in enumerate(results):
            day_start, day_end = boundaries[day]
            used[day_start:day_start + len(day_used)] = day_used
            total_scheduled += day_scheduled
        
        print(f"[DEBUG] {strategy}: scheduled {total_scheduled} jobs")
        return used

    def _schedule_carbon_responder(
        self,
        jobs: List[VMJob],
        price_per_mwh: np.ndarray,
        carbon_kg_per_mwh: np.ndarray,
        cfg: CarbonResponderConfig
    ) -> np.ndarray:
        """Postpone shiftable jobs within [release, release+max_shift] window to low-score hours.
        - Non-preemptive, constant-power jobs.
        - Capacity limit: IT capacity (config.capacity_mw).
        """
        # This part involves much more complex logic to find the best placement for each job
        # within the allowed time window, considering both price, carbon, and wait times.
        # We leave this part blank for now. No need to fill in the blank.

        return

    # ---------- Public: build facility demand after strategy ----------
    def demand_facility_mw(
        self,
        only_curtail: bool = False,
        use_battery: bool = False,
        carbon_responder: bool = False,
        curtailed_supply_mw: Optional[Sequence[float]] = None,  # facility-level for only_curtail
        price_vector_per_mwh: Optional[Sequence[float]] = None, # for carbon_responder
        carbon_vector_kg_per_mwh: Optional[Sequence[float]] = None,
        cr_cfg: Optional[CarbonResponderConfig] = None,
    ) -> np.ndarray:
        """
        Returns facility demand (MW) after applying a job-level strategy.
        Precedence: if both flags True, carbon_responder takes priority.
        """
        # Build scaled jobs
        if self._jobs is None:
            self._extract_jobs_from_vms()
        
        if self.scale_jobs:
            jobs = self._get_scaled_jobs()
        else:
            jobs = self._jobs
        H = self.config.week_hours

        # Determine base strategy
        if carbon_responder:
            cfg = cr_cfg or CarbonResponderConfig()
            price = np.zeros(H) if price_vector_per_mwh is None else np.array(price_vector_per_mwh, dtype=float)
            carbon = np.zeros(H) if carbon_vector_kg_per_mwh is None else np.array(carbon_vector_kg_per_mwh, dtype=float)
            it = self._schedule_carbon_responder(jobs, price, carbon, cfg)
        elif only_curtail:
            if curtailed_supply_mw is None:
                raise ValueError("curtailed_supply_mw must be provided for only_curtail strategy.")
            strategy = "curtail_battery" if use_battery else "curtail_only"
            it = self._schedule_parallel(jobs, np.array(curtailed_supply_mw, dtype=float), strategy, 
                                       price_vector_per_mwh if use_battery else None)
            print(f"[DEBUG] Curtail jobs input: {len(jobs)}, peak power: {max([j.it_power_mw for j in jobs]):.6f} MW")
        else:
            # as-is strategy - simple accumulation
            it = self._build_hourly_it_from_jobs_default(jobs)
            print(f"[DEBUG] As-is: scheduled {len(jobs)} jobs, peak power: {max([j.it_power_mw for j in jobs]):.6f} MW")

        return it * self.config.pue

    # ---------- Energy/cost/carbon simulation (unchanged, uses facility demand) ----------
    def simulate(
        self,
        curtailed_supply_mw: Optional[Sequence[float]] = None,   # facility-level MW (per hour)
        grid_price_per_mwh: float = 80.0,
        curtailed_price_per_mwh: float = 0.0,
        grid_ci_kg_per_mwh: float = 400.0,
        curtailed_ci_kg_per_mwh: float = 0.0,
        carbon_price_per_ton: float = 0.0,
        *,
        only_curtail: bool = False,
        carbon_responder: bool = False,
        price_vector_per_mwh: Optional[Sequence[float]] = None,
        carbon_vector_kg_per_mwh: Optional[Sequence[float]] = None,
        cr_cfg: Optional[CarbonResponderConfig] = None,
    ) -> pd.DataFrame:
        """
        Simulate one week using job-level workload shaping first (if enabled), then hourly energy accounting.
        """
        H = self.config.week_hours
        demand_mw = self.demand_facility_mw(
            only_curtail=only_curtail,
            carbon_responder=carbon_responder,
            curtailed_supply_mw=curtailed_supply_mw,
            price_vector_per_mwh=price_vector_per_mwh,
            carbon_vector_kg_per_mwh=carbon_vector_kg_per_mwh,
            cr_cfg=cr_cfg,
        )

        if curtailed_supply_mw is None:
            curtailed = np.zeros(H, dtype=float)
        else:
            curtailed = np.array(curtailed_supply_mw, dtype=float)
            if len(curtailed) != H:
                raise ValueError("curtailed_supply_mw must have length equal to week_hours.")

        log = {
            "hour": np.arange(H),
            "demand_mw": demand_mw.copy(),
            "met_by_curtail_mw": np.zeros(H),
            "battery_discharge_mw": np.zeros(H),
            "met_by_grid_mw": np.zeros(H),
            "battery_charge_mw": np.zeros(H),
            "battery_soc_mwh": np.zeros(H),
            "unserved_mw": np.zeros(H),
            "cost_usd": np.zeros(H),
            "emissions_kg": np.zeros(H),
        }

        # Get price vector for arbitrage
        if price_vector_per_mwh is not None:
            prices = np.array(price_vector_per_mwh[:H])
            avg_price = float(np.mean(prices))
        else:
            prices = np.full(H, grid_price_per_mwh)
            avg_price = grid_price_per_mwh
        
        for h in range(H):
            demand = demand_mw[h]
            curtail = max(float(curtailed[h]), 0.0)
            used_curtail = min(demand, curtail)
            log["met_by_curtail_mw"][h] = used_curtail

            deficit = max(demand - used_curtail, 0.0)
            surplus = max(curtail - used_curtail, 0.0)
            current_price = float(prices[h]) if h < len(prices) else avg_price

            if self.battery is not None:
                # Charge from surplus curtailment (free energy)
                if surplus > 0:
                    charged_mwh = self.battery.charge(request_mw=surplus, hours=1.0)
                    log["battery_charge_mw"][h] = charged_mwh
                    log["cost_usd"][h] += charged_mwh * curtailed_price_per_mwh
                    log["emissions_kg"][h] += charged_mwh * curtailed_ci_kg_per_mwh
                
                # Battery arbitrage: charge when price low
                elif float(current_price) < float(avg_price) * 0.7:
                    charge_power = self.battery.max_charge_mw
                    charged_mwh = self.battery.charge(request_mw=charge_power, hours=1.0)
                    if charged_mwh > 0:
                        log["battery_charge_mw"][h] = charged_mwh
                        log["met_by_grid_mw"][h] += charged_mwh
                        log["cost_usd"][h] += charged_mwh * current_price
                        log["emissions_kg"][h] += charged_mwh * grid_ci_kg_per_mwh
                
                # Discharge battery to meet deficit
                if deficit > 0:
                    discharged_mwh = self.battery.discharge(request_mw=deficit, hours=1.0)
                    log["battery_discharge_mw"][h] = discharged_mwh
                    deficit -= discharged_mwh
                
                # Discharge for arbitrage when price is high
                elif float(current_price) > float(avg_price) * 1.3 and self.battery.soc_mwh > 0:
                    discharge_power = min(self.battery.max_discharge_mw, self.battery.soc_mwh)
                    discharged_mwh = self.battery.discharge(request_mw=discharge_power, hours=1.0)
                    if discharged_mwh > 0:
                        log["battery_discharge_mw"][h] = discharged_mwh
                        log["met_by_grid_mw"][h] = max(0, demand - used_curtail - discharged_mwh)
                        log["cost_usd"][h] -= discharged_mwh * (float(current_price) - float(avg_price))
                
                log["battery_soc_mwh"][h] = self.battery.soc_mwh

            # Calculate final grid consumption and emissions
            final_grid_mw = max(0, demand - used_curtail - log["battery_discharge_mw"][h])
            log["met_by_grid_mw"][h] = final_grid_mw
            
            if final_grid_mw > 0:
                log["cost_usd"][h] += final_grid_mw * current_price
                energy_emissions_kg = final_grid_mw * grid_ci_kg_per_mwh
                log["emissions_kg"][h] += energy_emissions_kg
                if carbon_price_per_ton > 0:
                    log["cost_usd"][h] += (energy_emissions_kg / 1000.0) * carbon_price_per_ton

            # account curtailed energy used to meet demand
            log["cost_usd"][h] += used_curtail * curtailed_price_per_mwh
            log["emissions_kg"][h] += used_curtail * curtailed_ci_kg_per_mwh

            log["unserved_mw"][h] = max(demand - used_curtail - log["battery_discharge_mw"][h] - log["met_by_grid_mw"][h], 0.0)

        df = pd.DataFrame(log)
        totals = {
            "total_energy_mwh": float(df["demand_mw"].sum()),
            "it_energy_mwh": float(df["demand_mw"].sum() / self.config.pue),
            "met_by_curtail_mwh": float(df["met_by_curtail_mw"].sum() + df["battery_charge_mw"].sum()),
            "battery_discharge_mwh": float(df["battery_discharge_mw"].sum()),
            "grid_energy_mwh": float(df["met_by_grid_mw"].sum()),
            "unserved_mwh": float(df["unserved_mw"].sum()),
            "total_cost_usd": float(df["cost_usd"].sum()),
            "total_emissions_kg": float(df["emissions_kg"].sum()),
            "avg_power_mw": float(df["demand_mw"].mean()),
            "peak_power_mw": float(df["demand_mw"].max()),
        }
        df.attrs["totals"] = totals
        return df