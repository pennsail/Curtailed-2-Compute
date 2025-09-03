# datacenter.py
# Rewritten DataCenter with three strategies:
#   - as_is: run jobs at original times (capacity not enforced here by design)
#   - carbon_aware: shift within each day to lower-carbon hours, under IT capacity
#   - only_curtail: schedule only when curtailed IT is available; with battery, extend window after curtail ends
#
# Notes:
# - CSV is assumed to be "vmtable.csv" style WITHOUT header; we assign fixed headers.
# - Timestamps are seconds since epoch (INT). We pick the earliest created time as week start.
# - All scheduling operates in integer hours over a 7-day window (H=168).

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Sequence, List, Tuple, Dict
import numpy as np
import pandas as pd

from battery import Battery  # Local import if available


# ------------------------------
# Core config
# ------------------------------
@dataclass
class DataCenterConfig:
    capacity_mw: float = 20.0                # IT capacity (MW)
    pue: float = 1.2                         # Power Usage Effectiveness
    watts_per_vcpu: float = 20.0             # W@100% util per vCPU (IT power model coeff)
    utilization_column: str = "avg_cpu"      # 'avg_cpu' or 'p95 max cpu'
    week_hours: int = 7 * 24
    timezone: str = "America/Los_Angeles"    # epoch alignment


# ------------------------------
# Job model
# ------------------------------
@dataclass
class VMJob:
    # Times are integer hour indices relative to the simulated week window.
    release_h: int          # earliest (inclusive)
    end_orig_h: int         # original end (exclusive)
    duration_h: int         # ceil hours
    it_power_mw: float      # MW (IT)
    job_id: int             # index in CSV


# ------------------------------
# DataCenter with job-level strategies
# ------------------------------
@dataclass
class DataCenter:
    csv_path: str
    config: DataCenterConfig = field(default_factory=DataCenterConfig)
    battery: Optional[Battery] = None
    scale_jobs: bool = True

    _hourly_it_mw_raw: Optional[np.ndarray] = field(default=None, init=False, repr=False)
    _jobs: Optional[List[VMJob]] = field(default=None, init=False, repr=False)
    _week_start_epoch: Optional[float] = field(default=None, init=False, repr=False)

    # ---------- CSV ingestion ----------
    def _parse_csv(self) -> Tuple[pd.DataFrame, str, str]:
        # Handle vmtable.csv format with predefined headers (no header row in file)
        column_headers = [
            'vm id', 'subscription id', 'deployment id', 'timestamp vm created', 'timestamp vm deleted',
            'max cpu', 'avg cpu', 'p95 max cpu', 'vm category', 'vm virtual core count bucket', 'vm memory (gb) bucket'
        ]
        df = pd.read_csv(self.csv_path, header=None)
        if len(df.columns) != len(column_headers):
            raise ValueError(f"Expected {len(column_headers)} columns, found {len(df.columns)}")
            # df = pd.read_csv(self.csv_path)
            # if len(df.columns) != len(column_headers):
            #     # last resort: set names if fewer cols; user can adjust upstream
            #     df.columns = (df.columns.tolist() + column_headers[len(df.columns):])[:len(column_headers)]
        else:
            df.columns = column_headers

        df.columns = [c.strip().lower() for c in df.columns]
        required = ["vm id", "timestamp vm created", "timestamp vm deleted"]
        for col in required:
            if col not in df.columns:
                raise ValueError(f"Missing required column: {col}")

        util_col = self.config.utilization_column.lower()
        if util_col not in df.columns:
            # fallback to 'avg cpu' if requested column missing
            util_col = "avg cpu"

        vcpu_col = "vm virtual core count bucket"
        # coerce numeric types
        for col in [util_col, vcpu_col, "timestamp vm created", "timestamp vm deleted"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df, util_col, vcpu_col

    # ---------- Jobs extraction ----------
    def _extract_jobs_from_vms(self) -> List[VMJob]:
        df, util_col, vcpu_col = self._parse_csv()
        H = self.config.week_hours

        # Choose week window: start at earliest created timestamp in this CSV
        created_series = df["timestamp vm created"].dropna().astype(float)
        if created_series.empty:
            raise ValueError("No 'timestamp vm created' values.")
        week_start_epoch = float(created_series.min())
        self._week_start_epoch = week_start_epoch
        week_end_epoch = week_start_epoch + H * 3600.0

        # Clean rows
        # df = df.dropna(subset=[util_col, "timestamp vm created"])
        # # fill deleted with +1h minimum
        # df["timestamp vm deleted"] = df["timestamp vm deleted"].fillna(df["timestamp vm created"] + 3600.0)
        # # bounds
        # df = df[(df[util_col].between(0, 100))]
        # # vcpu bucket may be NaN; assume 2 if missing
        # df[vcpu_col] = df[vcpu_col].fillna(2).clip(lower=1)

        jobs: List[VMJob] = []
        watts_per_vcpu = float(self.config.watts_per_vcpu)

        for idx, row in df.iterrows():
            created = float(row["timestamp vm created"])
            deleted = float(row["timestamp vm deleted"])
            if deleted < created:
                raise ValueError(f"Invalid timestamps for job {idx}: {created} -> {deleted}")

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

            util_frac = float(row[util_col]) / 100.0
            vcpus = int(float(row[vcpu_col])) if not pd.isna(row[vcpu_col]) else 2

            it_power_mw = (watts_per_vcpu * vcpus * util_frac) / 1e6  # MW
            if it_power_mw <= 0.0:
                continue

            # # sanity check: skip jobs with duration > 12h
            # if duration_h > 12:
            #     continue

            jobs.append(VMJob(
                release_h=s_idx,
                end_orig_h=e_idx,
                duration_h=duration_h,
                it_power_mw=it_power_mw,
                job_id=int(idx)
            ))

        self._jobs = jobs
        return jobs

    # ---------- Build raw IT (as-is) ----------
    def _build_hourly_it_from_jobs_default(self, jobs: List[VMJob]) -> np.ndarray:
        """Sum IT power by hour, using original [release_h, end_orig_h) intervals."""
        H = self.config.week_hours
        it = np.zeros(H, dtype=float)
        if not jobs:
            return it

        starts = np.array([max(0, j.release_h) for j in jobs], dtype=int)
        ends = np.array([min(H, j.end_orig_h) for j in jobs], dtype=int)
        powers = np.array([j.it_power_mw for j in jobs], dtype=float)

        idx_list: List[int] = []
        val_list: List[float] = []
        for s, e, p in zip(starts, ends, powers):
            if e > s:
                idx_list.extend(range(s, e))
                val_list.extend([p] * (e - s))
        if idx_list:
            np.add.at(it, np.array(idx_list, dtype=int), np.array(val_list, dtype=float))
        return it

    # ---------- Utility: split jobs by day (segments within each day) ----------
    def _split_jobs_by_day(self, jobs: List[VMJob]) -> List[List[VMJob]]:
        """Return list of 7 lists; each inner list contains job segments confined to that day."""
        H = self.config.week_hours
        days: List[List[VMJob]] = [[] for _ in range(7)]
        for j in jobs:
            s = j.release_h
            e = j.end_orig_h
            if s >= H or e <= 0:
                continue
            s = max(0, s)
            e = min(H, e)
            # iterate over days overlapping this job
            day0 = s // 24
            day1 = (e - 1) // 24
            for d in range(day0, day1 + 1):
                day_start = d * 24
                day_end = min((d + 1) * 24, H)
                seg_s = max(s, day_start)
                seg_e = min(e, day_end)
                if seg_e > seg_s:
                    seg = VMJob(
                        release_h=seg_s - day_start,           # relative to day start
                        end_orig_h=seg_e - day_start,
                        duration_h=seg_e - seg_s,
                        it_power_mw=j.it_power_mw,
                        job_id=j.job_id,
                    )
                    days[d].append(seg)
        return days

    # ---------- Scaling: make peak IT ~ capacity ----------
    def _get_scaled_jobs(self) -> List[VMJob]:
        if self._jobs is None:
            self._extract_jobs_from_vms()
        if self._hourly_it_mw_raw is None:
            self._hourly_it_mw_raw = self._build_hourly_it_from_jobs_default(self._jobs or [])

        peak_raw = float(np.max(self._hourly_it_mw_raw)) if self._hourly_it_mw_raw.size else 0.0
        target = float(self.config.capacity_mw)
        if peak_raw <= 0 or target <= 0:
            return self._jobs or []

        k = target / peak_raw
        # multiplicative scaling on power keeps shapes & counts
        # save the scaled factor to a file, name it by strategy
        # with open(f"scale_factor_{self.config.strategy}.txt", "w") as f:
        #     f.write(f"{k}\n")

        return [
            VMJob(
                release_h=j.release_h,
                end_orig_h=j.end_orig_h,
                duration_h=j.duration_h,
                it_power_mw=j.it_power_mw * k,
                job_id=j.job_id,
            ) for j in (self._jobs or [])
        ]

    # ---------- Internal: greedy pack under per-hour capacity ----------
    @staticmethod
    def _fits_block(used: np.ndarray, cap: np.ndarray, start: int, power: float, dur: int) -> bool:
        end = start + dur
        if end > len(cap):  # bounds
            return False
        residual = cap[start:end] - used[start:end]
        return np.all(residual >= power - 1e-12)

    @staticmethod
    def _place_block(used: np.ndarray, start: int, power: float, dur: int) -> None:
        used[start:start+dur] += power

    # ---------- Strategy: carbon-aware (day-local shifting) ----------
    def _schedule_carbon_aware(self, jobs: List[VMJob], carbon_vec: np.ndarray) -> Tuple[np.ndarray, int]:
        """Within each day, shift job segments to lower-carbon hours without exceeding IT capacity."""
        H = self.config.week_hours
        it = np.zeros(H, dtype=float)
        cap_it = float(self.config.capacity_mw)
        per_day = self._split_jobs_by_day(jobs)
        scheduled_ids = set()

        for d in range(7):
            day_jobs = per_day[d]
            day_start = d * 24
            day_end = min(day_start + 24, H)
            L = day_end - day_start
            if L <= 0:
                continue
            used = np.zeros(L, dtype=float)
            cap = np.full(L, cap_it, dtype=float)
            carbon = np.array(carbon_vec[day_start:day_end], dtype=float)
            if carbon.size != L:
                carbon = np.full(L, np.mean(carbon) if carbon.size > 0 else 1.0)

            # Schedule longer jobs first to reduce fragmentation
            day_jobs_sorted = sorted(day_jobs, key=lambda j: j.duration_h, reverse=True)

            for j in day_jobs_sorted:
                dur = j.duration_h
                p = j.it_power_mw
                # Use carbon intensity sum as key, try candidate start points from low to high
                # Candidates are 0..L-dur
                if dur > L:
                    # Cannot place in this day, cross-day versions have been split, so skip directly
                    continue

                # Enumerate all feasible start points, sort by carbon sum first
                candidates: List[Tuple[float, int]] = []
                for s_rel in range(0, L - dur + 1):
                    csum = float(np.sum(carbon[s_rel:s_rel + dur]))
                    candidates.append((csum, s_rel))
                candidates.sort(key=lambda x: x[0])

                placed = False
                # Try low-carbon candidates first, check capacity
                for _, s_rel in candidates:
                    if self._fits_block(used, cap, s_rel, p, dur):
                        self._place_block(used, s_rel, p, dur)
                        scheduled_ids.add(j.job_id)
                        placed = True
                        break

                if not placed:
                    # Fall back to original position (relative to day), then try ±4h around original position
                    s0 = max(0, j.release_h)
                    s0 = min(s0, max(0, L - dur))
                    search = list(range(max(0, s0 - 4), min(L - dur, s0 + 4) + 1))
                    for s_rel in search:
                        if self._fits_block(used, cap, s_rel, p, dur):
                            self._place_block(used, s_rel, p, dur)
                            scheduled_ids.add(j.job_id)
                            placed = True
                            break
                    if not placed:
                        # Last resort: find first placeable position from left to right
                        for s_rel in range(0, L - dur + 1):
                            if self._fits_block(used, cap, s_rel, p, dur):
                                self._place_block(used, s_rel, p, dur)
                                scheduled_ids.add(j.job_id)
                                placed = True
                                break
                        # If still not possible, means daily total energy exceeds capacity, which should be rare after proper scaling

            it[day_start:day_end] = used
        return it, len(scheduled_ids)

    # ---------- Strategy: only_curtail (day-local packing; battery extends window) ----------
    # ---------- helpers for only_curtail ----------
    def _find_curtail_windows(self, fac_day: np.ndarray) -> list[tuple[int, int]]:
        """Return all continuous windows [start,end) where (facility) curtailment > 0 for the day."""
        pos = fac_day > 1e-9
        L = len(fac_day)
        i = 0
        wins = []
        while i < L:
            if pos[i]:
                j = i + 1
                while j < L and pos[j]:
                    j += 1
                wins.append((i, j))
                i = j
            else:
                i += 1
        return wins

    def _pack_nonpreemptive_blocks(
        self,
        jobs: list[VMJob],
        used_it: np.ndarray,
        cap_it: np.ndarray,
        day_start_abs: int,
        scheduled_ids: set[int],
    ) -> None:
        """
        Under cap_it (daily IT capacity limit vector), pack jobs (non-preemptive, fixed power, continuous duration_h hours)
        starting from job.release_h, find feasible continuous time slots; successful ones are added to scheduled_ids and update used_it.
        Greedy strategy: longer first, then higher power first.
        """
        L = cap_it.size
        # Long to short, high power to low power
        for j in sorted(jobs, key=lambda x: (x.duration_h, x.it_power_mw), reverse=True):
            if j.job_id in scheduled_ids:
                continue
            start_rel = max(0, j.release_h - day_start_abs)
            latest_rel = L - j.duration_h
            if latest_rel < start_rel:
                continue
            p = j.it_power_mw
            placed = False
            for s in range(start_rel, latest_rel + 1):
                seg = slice(s, s + j.duration_h)
                if np.all((cap_it[seg] - used_it[seg]) >= (p - 1e-12)):
                    used_it[seg] += p
                    scheduled_ids.add(j.job_id)
                    placed = True
                    break
            # If can't fit, leave for later (or next day backlog)

    # ---------- Strategy: only_curtail (with battery extension & backlog) ----------
    def _schedule_only_curtail(
        self,
        jobs: list[VMJob],
        curtailed_facility_mw: np.ndarray,
        use_battery: bool,
        *,
        reserve_frac_for_batt: float = 0.0,
        carry_backlog: bool = True,
    ) -> Tuple[np.ndarray, int, Dict[str, np.ndarray]]:
        """
        Battery strategy: Start with full battery, use it to extend curtailment windows earlier.
        """
        H = self.config.week_hours
        if curtailed_facility_mw.size != H:
            raise ValueError("curtailed_supply_mw must have length equal to week_hours.")

        pue = float(self.config.pue)
        it = np.zeros(H, dtype=float)
        have_batt = bool(use_battery and (self.battery is not None))
        
        # Start battery at full capacity
        if have_batt:
            self.battery.soc_mwh = self.battery.capacity_mwh

        jobs_by_day: list[list[VMJob]] = [[] for _ in range(7)]
        for j in jobs:
            day = min(max(j.release_h // 24, 0), 6)
            jobs_by_day[day].append(j)
        backlog: list[VMJob] = []
        scheduled_ids: set[int] = set()
        
        battery_charge_week = np.zeros(H, dtype=float)
        battery_discharge_week = np.zeros(H, dtype=float)
        battery_soc_week = np.zeros(H, dtype=float)

        for d in range(7):
            day_start = d * 24
            day_end = min(day_start + 24, H)
            L = day_end - day_start
            if L <= 0:
                continue

            curt_fac_day = np.array(curtailed_facility_mw[day_start:day_end], dtype=float)
            
            # Create extended capacity: curtailment + battery discharge at start of day
            extended_cap_it = np.zeros(L, dtype=float)
            
            # Add curtailment capacity
            extended_cap_it += curt_fac_day / pue
            
            # Add battery capacity throughout the day (not just beginning)
            if have_batt:
                # Distribute battery capacity across all hours of the day
                daily_battery_energy = min(self.battery.soc_mwh, self.battery.max_discharge_mw * L)
                if daily_battery_energy > 0:
                    battery_power_per_hour = daily_battery_energy / L / pue  # IT MW per hour
                    extended_cap_it += battery_power_per_hour
            
            used_it = np.zeros(L, dtype=float)
            day_jobs = (backlog if carry_backlog else []) + jobs_by_day[d]
            
            # Schedule jobs with extended capacity
            self._pack_nonpreemptive_blocks(
                jobs=day_jobs,
                used_it=used_it,
                cap_it=extended_cap_it,
                day_start_abs=day_start,
                scheduled_ids=scheduled_ids,
            )
            
            # Discharge battery for hours that used battery capacity
            if have_batt:
                for h in range(L):
                    curtail_cap = curt_fac_day[h] / pue
                    if used_it[h] > curtail_cap:
                        battery_usage_it = used_it[h] - curtail_cap
                        battery_usage_fac = battery_usage_it * pue
                        if battery_usage_fac > 0:
                            delivered = self.battery.discharge(request_mw=battery_usage_fac, hours=1.0)
                            battery_discharge_week[day_start + h] = delivered
            
            # Charge battery with surplus curtailment
            if have_batt:
                for h in range(L):
                    if curt_fac_day[h] > 1e-12:
                        used_fac = used_it[h] * pue
                        surplus_fac = max(curt_fac_day[h] - used_fac, 0.0)
                        if surplus_fac > 1e-12:
                            charged = self.battery.charge(request_mw=surplus_fac, hours=1.0)
                            battery_charge_week[day_start + h] = charged
            
            # Record SOC
            for h in range(L):
                battery_soc_week[day_start + h] = self.battery.soc_mwh if have_batt else 0.0
            
            # Backlog
            if carry_backlog:
                backlog = [j for j in day_jobs if j.job_id not in scheduled_ids]
            else:
                backlog = []
            
            it[day_start:day_end] = used_it

        battery_usage = {
            'charge_mw': battery_charge_week,
            'discharge_mw': battery_discharge_week,
            'soc_mwh': battery_soc_week
        }
        
        return it, len(scheduled_ids), battery_usage

    # ---------- Public: build facility demand after strategy ----------
    def demand_facility_mw(
        self,
        strategy: str = "as_is",  # "as_is", "only_curtail", "carbon_aware"
        use_battery: bool = False,
        curtailed_supply_mw: Optional[Sequence[float]] = None,   # length 168 (facility MW)
        price_vector_per_mwh: Optional[Sequence[float]] = None,  # (unused here; reserved)
        carbon_vector_kg_per_mwh: Optional[Sequence[float]] = None,  # length 168
        reserve_frac_for_batt: Optional[float] = None,
        carry_backlog: Optional[bool] = None
    ) -> Tuple[np.ndarray, int, Dict[str, np.ndarray]]:
        """
        Returns (facility demand (MW), jobs_scheduled_count, battery_usage_dict) after applying a job-level strategy.
        Battery does not affect scheduling for 'as_is' and 'carbon_aware'; it affects energy/carbon
        in subsequent accounting. For 'only_curtail', battery (if provided and use_battery=True)
        extends the within-day executable window by discharging after curtail hours.
        
        battery_usage_dict contains:
        - 'charge_mw': hourly battery charging (MW)
        - 'discharge_mw': hourly battery discharging (MW)
        - 'soc_mwh': hourly battery state of charge (MWh)
        """
        # Prepare jobs (extraction + scaling)
        if self._jobs is None:
            self._extract_jobs_from_vms()
        jobs = self._get_scaled_jobs() if self.scale_jobs else (self._jobs or [])
        H = self.config.week_hours

        strategy = (strategy or "as_is").lower().strip()
        if strategy not in ("as_is", "only_curtail", "carbon_aware"):
            raise ValueError("strategy must be one of: 'as_is', 'only_curtail', 'carbon_aware'")

        if strategy == "as_is":
            it = self._build_hourly_it_from_jobs_default(jobs)
            jobs_scheduled = len(jobs)

        elif strategy == "carbon_aware":
            if carbon_vector_kg_per_mwh is None:
                raise ValueError("carbon_aware requires carbon_vector_kg_per_mwh (length 168).")
            carbon = np.array(carbon_vector_kg_per_mwh, dtype=float)
            if carbon.size != H:
                raise ValueError("carbon_vector_kg_per_mwh must have length 168.")
            it, jobs_scheduled = self._schedule_carbon_aware(jobs, carbon)

        else:  # "only_curtail"
            if curtailed_supply_mw is None:
                raise ValueError("only_curtail requires curtailed_supply_mw (length 168).")
            curtailed = np.array(curtailed_supply_mw, dtype=float)
            if curtailed.size != H:
                raise ValueError("curtailed_supply_mw must have length 168.")
            it, jobs_scheduled, battery_usage = self._schedule_only_curtail(jobs, curtailed, use_battery=use_battery,
                                                             carry_backlog=carry_backlog)

        # Facility power = IT * PUE
        facility_demand = it * float(self.config.pue)
        
        # Battery usage tracking (only meaningful for only_curtail strategy)
        if strategy != "only_curtail":
            battery_usage = {
                'charge_mw': np.zeros(H, dtype=float),
                'discharge_mw': np.zeros(H, dtype=float), 
                'soc_mwh': np.zeros(H, dtype=float)
            }
        
        return facility_demand, jobs_scheduled, battery_usage
        
    def simulate(
        self,
        *,
        strategy: str = "as_is",
        use_battery: bool = False,
        curtailed_supply_mw: Optional[Sequence[float]] = None,
        price_vector_per_mwh: Optional[Sequence[float]] = None,
        carbon_vector_kg_per_mwh: Optional[Sequence[float]] = None,
        curtailed_price_per_mwh: float = 0.0,
        carbon_price_per_ton: float = 0.0,
        reserve_frac_for_batt: Optional[float] = None,
        carry_backlog: Optional[bool] = None
    ) -> pd.DataFrame:
        """
        Perform job scheduling and energy accounting.
        Battery strategies:
        - as_is/carbon_aware: Grid arbitrage (charge when cheap, discharge when expensive)
        - only_curtail: Extend job execution window (charge from surplus curtailment, discharge to extend window)
        """
        H = self.config.week_hours
        
        # Prepare input vectors
        if curtailed_supply_mw is None:
            curtailed = np.zeros(H, dtype=float)
        else:
            curtailed = np.asarray(curtailed_supply_mw, dtype=float)
            if curtailed.size != H:
                raise ValueError("curtailed_supply_mw length must equal week_hours.")

        if price_vector_per_mwh is not None:
            prices = np.asarray(price_vector_per_mwh, dtype=float)
            if prices.size < H:
                raise ValueError("price_vector_per_mwh must have length >= week_hours.")
        else:
            raise ValueError("price_vector_per_mwh is required for simulate().")

        if carbon_vector_kg_per_mwh is not None:
            grid_ci = np.asarray(carbon_vector_kg_per_mwh, dtype=float)
            if grid_ci.size < H:
                raise ValueError("carbon_vector_kg_per_mwh must have length >= week_hours.")
        else:
            grid_ci = np.full(H, 400.0, dtype=float)

        # Get job scheduling demand and battery usage
        demand_mw, jobs_scheduled, battery_usage = self.demand_facility_mw(
            strategy=strategy,
            use_battery=use_battery,
            curtailed_supply_mw=curtailed,
            price_vector_per_mwh=prices,
            carbon_vector_kg_per_mwh=grid_ci,
            reserve_frac_for_batt=reserve_frac_for_batt,
            carry_backlog=carry_backlog
        )

        if demand_mw.size != H:
            raise ValueError("demand_facility_mw must return an array of length week_hours.")

        # Battery operations based on strategy
        if self.battery is not None and use_battery:
            if strategy == "only_curtail":
                # For only_curtail: use battery usage from scheduling phase
                battery_charge_mw = battery_usage['charge_mw']
                battery_discharge_mw = battery_usage['discharge_mw']
                battery_soc_mwh = battery_usage['soc_mwh']
            else:
                # For as_is/carbon_aware: grid arbitrage (charge when cheap, discharge when expensive)
                battery_charge_mw = np.zeros(H, dtype=float)
                battery_discharge_mw = np.zeros(H, dtype=float)
                battery_soc_mwh = np.zeros(H, dtype=float)
                
                # Simple strategy: charge when price < median, discharge when price > median
                median_price = np.median(prices[:H])
                for h in range(H):
                    if prices[h] < median_price:
                        # Charge up to 50% of demand or battery capacity
                        charge_target = min(demand_mw[h] * 0.5, self.battery.max_charge_mw)
                        charged = self.battery.charge(request_mw=charge_target, hours=1.0)
                        battery_charge_mw[h] = charged
                    elif prices[h] > median_price:
                        # Discharge up to 50% of demand or available energy
                        discharge_target = min(demand_mw[h] * 0.5, self.battery.max_discharge_mw)
                        discharged = self.battery.discharge(request_mw=discharge_target, hours=1.0)
                        battery_discharge_mw[h] = discharged
                    battery_soc_mwh[h] = self.battery.soc_mwh
        else:
            # No battery
            battery_charge_mw = np.zeros(H, dtype=float)
            battery_discharge_mw = np.zeros(H, dtype=float)
            battery_soc_mwh = np.zeros(H, dtype=float)

        # Calculate net grid demand (demand - battery discharge + battery charge)
        net_grid_demand_mw = demand_mw - battery_discharge_mw + battery_charge_mw
        
        # All energy comes from grid (curtailment has same price/carbon as grid)
        met_by_grid_mw = net_grid_demand_mw.copy()
        met_by_curtail_mw = np.zeros(H, dtype=float)  # Not used in cost calculation

        # Calculate costs and emissions (all from grid, including curtailment)
        total_cost_usd = float(np.dot(met_by_grid_mw, prices[:H]))
        total_carbon_kg = float(np.dot(met_by_grid_mw, grid_ci[:H]))
        
        # Add carbon pricing if applicable
        if carbon_price_per_ton > 0.0:
            # total_cost_usd += (total_carbon_kg / 1000.0) * carbon_price_per_ton
            raise NotImplementedError("Carbon pricing not implemented")

        # Create result DataFrame
        log = {
            "hour": np.arange(H),
            "demand_mw": demand_mw.astype(float),
            "battery_charge_mw": battery_charge_mw,
            "battery_discharge_mw": battery_discharge_mw,
            "battery_soc_mwh": battery_soc_mwh,
            "met_by_curtail_mw": met_by_curtail_mw,
            "met_by_grid_mw": met_by_grid_mw,
            "cost_usd": met_by_grid_mw * prices[:H],
            "emissions_kg": met_by_grid_mw * grid_ci[:H],
        }
        
        df = pd.DataFrame(log)
        
        # Summary totals
        pue = float(self.config.pue)
        totals = {
            "jobs_scheduled": int(jobs_scheduled),
            "total_energy_mwh": float(df["demand_mw"].sum()),
            "it_energy_mwh": float(df["demand_mw"].sum() / pue),
            "curtail_energy_to_load_mwh": 0.0,  # Not tracked separately
            "battery_charge_mwh": float(df["battery_charge_mw"].sum()),
            "battery_discharge_mwh": float(df["battery_discharge_mw"].sum()),
            "grid_energy_mwh": float(df["met_by_grid_mw"].sum()),
            "total_cost_usd": total_cost_usd,
            "total_emissions_kg": total_carbon_kg,
            "avg_power_mw": float(df["demand_mw"].mean()),
            "peak_power_mw": float(df["demand_mw"].max()),
        }
        df.attrs["totals"] = totals
        return df
