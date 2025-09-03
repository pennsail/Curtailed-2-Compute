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
        reserve_frac_for_batt: float = 0.0,   # No reservation - use only surplus curtailment
        carry_backlog: bool = True,           # Carry jobs not scheduled from previous day to next day
    ) -> Tuple[np.ndarray, int]:
        """
        Run jobs only when there is curtailment or "battery tail segment charged by curtailment".
        Battery strategy: Use surplus curtailment (after meeting job demand) to charge battery,
        then discharge at end of curtailment to extend job execution window.
        """
        H = self.config.week_hours
        if curtailed_facility_mw.size != H:
            raise ValueError("curtailed_supply_mw must have length equal to week_hours.")

        pue = float(self.config.pue)
        it = np.zeros(H, dtype=float)
        have_batt = bool(use_battery and (self.battery is not None))

        # First divide by day according to release_h; also prepare backlog
        jobs_by_day: list[list[VMJob]] = [[] for _ in range(7)]
        for j in jobs:
            day = min(max(j.release_h // 24, 0), 6)
            jobs_by_day[day].append(j)
        backlog: list[VMJob] = []

        scheduled_ids: set[int] = set()

        for d in range(7):
            day_start = d * 24
            day_end = min(day_start + 24, H)
            L = day_end - day_start
            if L <= 0:
                continue

            curt_fac_day = np.array(curtailed_facility_mw[day_start:day_end], dtype=float)
            # All curtailment available for jobs (no reservation)
            cap_it_curtail = curt_fac_day / pue
            used_it = np.zeros(L, dtype=float)

            # Jobs to schedule = backlog (cross-day) + today's released jobs
            day_jobs = (backlog if carry_backlog else []) + jobs_by_day[d]
            
            # --- 1) First pack jobs into curtailment windows ---
            self._pack_nonpreemptive_blocks(
                jobs=day_jobs,
                used_it=used_it,
                cap_it=cap_it_curtail,
                day_start_abs=day_start,
                scheduled_ids=scheduled_ids,
            )

            # --- 2) Use surplus curtailment to charge battery ---
            if have_batt:
                for h in range(L):
                    if curt_fac_day[h] > 1e-12:
                        # Calculate surplus after job usage
                        used_fac = used_it[h] * pue
                        surplus_fac = max(curt_fac_day[h] - used_fac, 0.0)
                        if surplus_fac > 1e-12:
                            self.battery.charge(request_mw=float(surplus_fac), hours=1.0)

            # --- 3) Calculate tail segment (after last curtailment window ends) ---
            tail_it_cap = np.zeros(L, dtype=float)
            if have_batt:
                windows = self._find_curtail_windows(curt_fac_day)
                if windows:
                    last_end_rel = windows[-1][1]
                else:
                    last_end_rel = 0  # If no curtailment today, treat tail as starting from 0
                
                # Discharge hourly until no power or end of day
                for rel_h in range(last_end_rel, L):
                    delivered = self.battery.discharge(
                        request_mw=self.battery.max_discharge_mw, hours=1.0
                    )
                    if delivered <= 1e-12:
                        break
                    tail_it_cap[rel_h] = delivered / pue  # MWh@1h -> MW

            # --- 4) Use "total IT capacity = curtailment IT + tail IT" to pack remaining jobs ---
            total_cap_it = cap_it_curtail + tail_it_cap
            remaining_jobs = [j for j in day_jobs if j.job_id not in scheduled_ids and j.duration_h <= L]
            
            # Pack remaining jobs using total capacity
            self._pack_nonpreemptive_blocks(
                jobs=remaining_jobs,
                used_it=used_it,
                cap_it=total_cap_it,
                day_start_abs=day_start,
                scheduled_ids=scheduled_ids,
            )

            # --- 5) Generate next day's backlog ---
            if carry_backlog:
                backlog = [j for j in day_jobs if j.job_id not in scheduled_ids and j.duration_h <= 24]
            else:
                backlog = []

            # --- 6) Write back daily IT demand ---
            it[day_start:day_end] = used_it

        return it, len(scheduled_ids)

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
    ) -> Tuple[np.ndarray, int]:
        """
        Returns (facility demand (MW), jobs_scheduled_count) after applying a job-level strategy.
        Battery does not affect scheduling for 'as_is' and 'carbon_aware'; it affects energy/carbon
        in subsequent accounting. For 'only_curtail', battery (if provided and use_battery=True)
        extends the within-day executable window by discharging after curtail hours.
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
            it, jobs_scheduled = self._schedule_only_curtail(jobs, curtailed, use_battery=use_battery,
                                                             carry_backlog=carry_backlog)

        # Facility power = IT * PUE
        return it * float(self.config.pue), jobs_scheduled
        
    def simulate(
        self,
        *,
        strategy: str = "as_is",                  # "as_is" | "only_curtail" | "carbon_aware"
        use_battery: bool = False,                # Only affects scheduling phase strategy (e.g., extend window for only_curtail); accounting phase will charge/discharge based on actual battery SoC
        curtailed_supply_mw: Optional[Sequence[float]] = None,  # Facility-side curtailment power vector, length=week_hours
        price_vector_per_mwh: Optional[Sequence[float]] = None, # Hourly electricity price ($/MWh), used for cost accounting
        carbon_vector_kg_per_mwh: Optional[Sequence[float]] = None,  # Hourly carbon intensity (kg CO2 / MWh), only applies to "grid power supply"
        curtailed_price_per_mwh: float = 0.0,    # Curtailed electricity cost (usually 0)
        carbon_price_per_ton: float = 0.0,       # Carbon price ($/tCO2), can be 0
        reserve_frac_for_batt: Optional[float] = None,
        carry_backlog: Optional[bool] = None
    ) -> pd.DataFrame:
        """
        Under given strategy, first perform job scheduling (get hourly facility demand), then do hourly energy/cost/carbon accounting.
        Battery strategy: only charge with "curtailment surplus"; discharge by power/SoC when there's deficit. Remaining deficit supplied by grid.
        Note: No grid arbitrage charging here to ensure carbon results are only determined by scheduling and curtailment/BESS effects.

        Returns: DataFrame (168 rows), containing hourly breakdown and .attrs["totals"] summary.
        """
        H = self.config.week_hours
        # --------- Prepare input vectors ---------
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
            # If not provided, use a conservative constant (not recommended for long-term use)
            grid_ci = np.full(H, 400.0, dtype=float)

        # --------- First do job scheduling → get hourly facility demand & job count ---------
        # Your demand_facility_mw should return (facility_mw_array, jobs_scheduled)
        demand_mw, jobs_scheduled = self.demand_facility_mw(
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

        # --------- Initialize log ---------
        log = {
            "hour": np.arange(H),
            "demand_mw": demand_mw.astype(float).copy(),
            "met_by_curtail_mw": np.zeros(H, dtype=float),
            "battery_charge_mw": np.zeros(H, dtype=float),     # Record as "power equivalent" (1h step → MWh == MW)
            "battery_discharge_mw": np.zeros(H, dtype=float),
            "met_by_grid_mw": np.zeros(H, dtype=float),
            "spilled_curtail_mw": np.zeros(H, dtype=float),
            "battery_soc_mwh": np.zeros(H, dtype=float),
            "cost_usd": np.zeros(H, dtype=float),
            "emissions_kg": np.zeros(H, dtype=float),
        }

        # # --------- Battery initial state ---------
        # if reset_battery_soc and self.battery is not None:
        #     self.battery.reset_soc(0.0)

        pue = float(self.config.pue)

        # --------- Hourly accounting ---------
        for h in range(H):
            demand = demand_mw[h]                 # This hour's facility-side demand (MW)
            curtail_fac = max(curtailed[h], 0.0)  # This hour's facility-side available curtailment (MW)

            # 1) First use curtailment to directly meet demand
            use_from_curtail = min(demand, curtail_fac)
            log["met_by_curtail_mw"][h] = use_from_curtail

            # 2) Calculate surplus curtailment and use surplus to charge battery (if any)
            surplus_curtail = max(curtail_fac - use_from_curtail, 0.0)  # Facility-side MW
            if self.battery is not None and surplus_curtail > 1e-12:
                charged_mwh = self.battery.charge(request_mw=surplus_curtail, hours=1.0)  # Returns input energy MWh
                log["battery_charge_mw"][h] = charged_mwh  # 1 hour step → record directly as MW
                # Curtailed energy for charging counts as curtailed cost/carbon (usually 0)
                log["cost_usd"][h] += charged_mwh * float(curtailed_price_per_mwh)
                # Curtailed energy treated as 0 carbon, can pass negative marginal carbon here if needed
                # Keep separate from grid_ci: curtailed energy doesn't multiply by grid_ci
            else:
                # No battery or can't charge, record curtailed overflow as spilled
                log["spilled_curtail_mw"][h] = surplus_curtail

            # 3) If curtailment insufficient, use battery discharge → grid makes up deficit
            remaining = demand - use_from_curtail
            if remaining > 1e-12 and self.battery is not None:
                discharged_mwh = self.battery.discharge(request_mw=remaining, hours=1.0)
                log["battery_discharge_mw"][h] = discharged_mwh
                remaining = max(remaining - discharged_mwh, 0.0)

            # 4) Remaining deficit supplied by grid, calculate cost/carbon price (curtailed energy doesn't count grid carbon)
            if remaining > 1e-12:
                log["met_by_grid_mw"][h] = remaining
                price = float(prices[h])
                ci = float(grid_ci[h])
                # Direct cost
                log["cost_usd"][h] += remaining * price
                # Grid energy corresponding carbon
                energy_emissions_kg = remaining * ci
                log["emissions_kg"][h] += energy_emissions_kg
                # Carbon price (if any)
                if carbon_price_per_ton > 0.0:
                    log["cost_usd"][h] += (energy_emissions_kg / 1000.0) * float(carbon_price_per_ton)

            # 5) Curtailed energy supplied to load (not charging) calculates curtailed cost (usually 0 carbon/0 cost)
            if use_from_curtail > 1e-12:
                log["cost_usd"][h] += use_from_curtail * float(curtailed_price_per_mwh)
                # If you have non-zero curtailed carbon intensity, can add here; currently assumes 0.

            # Record SoC
            if self.battery is not None:
                log["battery_soc_mwh"][h] = self.battery.soc_mwh
            else:
                log["battery_soc_mwh"][h] = 0.0

        df = pd.DataFrame(log)

        # --------- Summary ---------
        totals = {
            "jobs_scheduled": int(jobs_scheduled),
            "total_energy_mwh": float(df["demand_mw"].sum()),
            "it_energy_mwh": float(df["demand_mw"].sum() / pue),
            "curtail_energy_to_load_mwh": float(df["met_by_curtail_mw"].sum()),
            "battery_charge_mwh": float(df["battery_charge_mw"].sum()),
            "battery_discharge_mwh": float(df["battery_discharge_mw"].sum()),
            "grid_energy_mwh": float(df["met_by_grid_mw"].sum()),
            "spilled_curtail_mwh": float(df["spilled_curtail_mw"].sum()),
            "total_cost_usd": float(df["cost_usd"].sum()),
            "total_emissions_kg": float(df["emissions_kg"].sum()),
            "avg_power_mw": float(df["demand_mw"].mean()),
            "peak_power_mw": float(df["demand_mw"].max()),
        }
        df.attrs["totals"] = totals
        return df
