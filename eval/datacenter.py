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
    total_mw = 20.0
    pue: float = 1.2                         # Power Usage Effectiveness
    capacity_mw: float = total_mw / pue  # IT capacity (MW)
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
        df = pd.read_csv(self.csv_path)
        df.columns = [c.strip().lower() for c in df.columns]
        required = ["vm_id", "timestamp vm created", "timestamp vm deleted"]
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
        H = self.config.week_hours
        it = np.zeros(H, dtype=float)
        for j in jobs:
            # default: run as originally overlapped
            it[j.release_h : j.end_orig_h] += j.it_power_mw
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
    def _schedule_only_curtail(self, jobs: List[VMJob], curtailed_facility_mw: np.ndarray) -> np.ndarray:
        """Schedule jobs only during curtailment windows."""
        H = self.config.week_hours
        cap_it_by_curtail = curtailed_facility_mw / self.config.pue
        used = np.zeros(H, dtype=float)
        
        # Find all curtailment windows
        curtail_windows = []
        h = 0
        while h < H:
            if cap_it_by_curtail[h] > 0:
                start = h
                while h < H and cap_it_by_curtail[h] > 0:
                    h += 1
                curtail_windows.append((start, h))
            else:
                h += 1
        
        # Sort jobs by release time, then by power
        sorted_jobs = sorted(jobs, key=lambda j: (j.release_h, j.it_power_mw))
        
        # Try to schedule each job in curtailment windows
        for job in sorted_jobs:
            scheduled = False
            
            # Try each curtailment window
            for window_start, window_end in curtail_windows:
                window_length = window_end - window_start
                
                # Job must fit in window
                if job.duration_h > window_length:
                    continue
                
                # Try to place job at each position in window
                for start_h in range(window_start, window_end - job.duration_h + 1):
                    can_place = True
                    
                    # Check if enough capacity for entire duration
                    for h in range(start_h, start_h + job.duration_h):
                        if cap_it_by_curtail[h] - used[h] < job.it_power_mw:
                            can_place = False
                            break
                    
                    if can_place:
                        # Place the job
                        for h in range(start_h, start_h + job.duration_h):
                            used[h] += job.it_power_mw
                        scheduled = True
                        break
                
                if scheduled:
                    break
        
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

        if carbon_responder:
            cfg = cr_cfg or CarbonResponderConfig()
            price = np.zeros(H) if price_vector_per_mwh is None else np.array(price_vector_per_mwh, dtype=float)
            carbon = np.zeros(H) if carbon_vector_kg_per_mwh is None else np.array(carbon_vector_kg_per_mwh, dtype=float)
            it = self._schedule_carbon_responder(jobs, price, carbon, cfg)

        elif only_curtail:
            if curtailed_supply_mw is None:
                raise ValueError("curtailed_supply_mw must be provided for only_curtail strategy.")
            it = self._schedule_only_curtail(jobs, np.array(curtailed_supply_mw, dtype=float))

        else:
            # run-as-is (original timestamps)
            it = self._build_hourly_it_from_jobs_default(jobs)

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

        for h in range(H):
            demand = demand_mw[h]
            curtail = max(curtailed[h], 0.0)
            used_curtail = min(demand, curtail)
            log["met_by_curtail_mw"][h] = used_curtail

            deficit = max(demand - used_curtail, 0.0)
            surplus = max(curtail - used_curtail, 0.0)

            if self.battery is not None:
                if surplus > 0:
                    charged_mwh = self.battery.charge(request_mw=surplus, hours=1.0)
                    log["battery_charge_mw"][h] = charged_mwh
                    log["cost_usd"][h] += charged_mwh * curtailed_price_per_mwh
                    log["emissions_kg"][h] += charged_mwh * curtailed_ci_kg_per_mwh
                if deficit > 0:
                    discharged_mwh = self.battery.discharge(request_mw=deficit, hours=1.0)
                    log["battery_discharge_mw"][h] = discharged_mwh
                    deficit -= discharged_mwh
                log["battery_soc_mwh"][h] = self.battery.soc_mwh

            if deficit > 0:
                log["met_by_grid_mw"][h] = deficit
                log["cost_usd"][h] += deficit * grid_price_per_mwh
                energy_emissions_kg = deficit * grid_ci_kg_per_mwh
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
