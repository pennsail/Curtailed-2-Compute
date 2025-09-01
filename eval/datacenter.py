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
            # best-effort: try without forcing header if shape differs
            df = pd.read_csv(self.csv_path)
            if len(df.columns) != len(column_headers):
                # last resort: set names if fewer cols; user can adjust upstream
                df.columns = (df.columns.tolist() + column_headers[len(df.columns):])[:len(column_headers)]
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
        df = df.dropna(subset=[util_col, "timestamp vm created"])
        # fill deleted with +1h minimum
        df["timestamp vm deleted"] = df["timestamp vm deleted"].fillna(df["timestamp vm created"] + 3600.0)
        # bounds
        df = df[(df[util_col].between(0, 100))]
        # vcpu bucket may be NaN; assume 2 if missing
        df[vcpu_col] = df[vcpu_col].fillna(2).clip(lower=1)

        jobs: List[VMJob] = []
        watts_per_vcpu = float(self.config.watts_per_vcpu)

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

            util_frac = float(row[util_col]) / 100.0
            vcpus = int(float(row[vcpu_col])) if not pd.isna(row[vcpu_col]) else 2

            it_power_mw = (watts_per_vcpu * vcpus * util_frac) / 1e6  # MW
            if it_power_mw <= 0.0:
                continue

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

            # 長作業先排，降低碎片化
            day_jobs_sorted = sorted(day_jobs, key=lambda j: j.duration_h, reverse=True)

            for j in day_jobs_sorted:
                dur = j.duration_h
                p = j.it_power_mw
                # 以碳強度和為 key，從小到大試候選起點
                # 候選為 0..L-dur
                if dur > L:
                    # 無法在本日安放，退回原位跨日的版本早已被拆分，因此直接略過
                    continue

                # 列舉所有可行起點，先按碳和排序
                candidates: List[Tuple[float, int]] = []
                for s_rel in range(0, L - dur + 1):
                    csum = float(np.sum(carbon[s_rel:s_rel + dur]))
                    candidates.append((csum, s_rel))
                candidates.sort(key=lambda x: x[0])

                placed = False
                # 先試低碳候選，檢查容量
                for _, s_rel in candidates:
                    if self._fits_block(used, cap, s_rel, p, dur):
                        self._place_block(used, s_rel, p, dur)
                        scheduled_ids.add(j.job_id)
                        placed = True
                        break

                if not placed:
                    # 落回原始位置（相對當日），再從原位附近 ±4h 嘗試
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
                        # 最後保底：左到右找第一個能放的
                        for s_rel in range(0, L - dur + 1):
                            if self._fits_block(used, cap, s_rel, p, dur):
                                self._place_block(used, s_rel, p, dur)
                                scheduled_ids.add(j.job_id)
                                placed = True
                                break
                        # 若仍不行，代表本日總能量確實超容量，這在正常縮放後理論上少見

            it[day_start:day_end] = used
        return it, len(scheduled_ids)

    # ---------- Strategy: only_curtail (day-local packing; battery extends window) ----------
    # ---------- helpers for only_curtail ----------
    def _find_curtail_windows(self, fac_day: np.ndarray) -> list[tuple[int, int]]:
        """回傳當日所有（facility）棄電>0 的連續窗口 [start,end)。"""
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
        在 cap_it（當日 IT 容量上限向量）下，將 jobs（非搶占、定功率、連續 duration_h 小時）
        自 job.release_h 起，找可行的連續時段塞入；成功者加到 scheduled_ids 並更新 used_it。
        貪婪策略：先長後短、功率大的先試。
        """
        L = cap_it.size
        # 長→短，功率大→小
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
            # 放不下就留待後面（或隔天 backlog）

    # ---------- Strategy: only_curtail (with battery extension & backlog) ----------
    def _schedule_only_curtail(
        self,
        jobs: list[VMJob],
        curtailed_facility_mw: np.ndarray,
        use_battery: bool,
        *,
        reserve_frac_for_batt: float = 0.20,  # 每個有棄電小時，預留 20% 功率給電池充電
        carry_backlog: bool = True,           # 前一天沒排到的作業帶到隔天
    ) -> Tuple[np.ndarray, int]:
        """
        只在有棄電或「棄電充好的電池尾段」時運行作業。
        核心差異：
          1) 棄電小時先預留一部分功率（facility）用來充電，剩餘才拿來當下跑作業。
          2) 充完後計算尾段能放的「等效 IT 容量向量」，把「剩餘作業」一次性打包進
             [當日任何時段含尾段] 的總容量，避免雙峰。
          3) 可選 backlog，將未排入的短作業帶到隔天。
        """
        H = self.config.week_hours
        if curtailed_facility_mw.size != H:
            raise ValueError("curtailed_supply_mw must have length equal to week_hours.")
        if not (0.0 <= reserve_frac_for_batt < 1.0):
            raise ValueError("reserve_frac_for_batt must be in [0,1).")

        pue = float(self.config.pue)
        it = np.zeros(H, dtype=float)
        have_batt = bool(use_battery and (self.battery is not None))

        # 依 release_h 先分日；另外準備 backlog
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

            # --- 1) 為充電預留一部分 facility 棄電，剩餘才轉成 IT 可用 ---
            reserve_fac = np.where(curt_fac_day > 0, curt_fac_day * reserve_frac_for_batt, 0.0)
            run_fac = curt_fac_day - reserve_fac
            # 當下可用的 IT 容量（只靠「不預留」那部分棄電）
            cap_it_now = run_fac / pue

            used_it = np.zeros(L, dtype=float)

            # 待排作業 = backlog（跨日） + 今天釋出的
            day_jobs = (backlog if carry_backlog else []) + jobs_by_day[d]
            # 先把「能落在有棄電的時段」的作業塞到 cap_it_now（整天都可放，但實際只有棄電小時 cap_it_now>0）
            self._pack_nonpreemptive_blocks(
                jobs=day_jobs,
                used_it=used_it,
                cap_it=cap_it_now,
                day_start_abs=day_start,
                scheduled_ids=scheduled_ids,
            )

            # --- 2) 用預留的 reserve_fac 逐小時為電池充電（facility）---
            if have_batt:
                for h in range(L):
                    if reserve_fac[h] > 1e-12:
                        self.battery.charge(request_mw=float(reserve_fac[h]), hours=1.0)

            # Update backlog for next day
            if carry_backlog:
                backlog = [j for j in day_jobs if j.job_id not in scheduled_ids and j.duration_h <= 24]

            it[day_start:day_end] = used_it

            #  --- 3) 計算尾段（從最後一個棄電窗結束後），把電池能量化為「IT 尾段容量」 ---
            tail_it_cap = np.zeros(L, dtype=float)
            if have_batt:
                windows = self._find_curtail_windows(curt_fac_day)
                if windows:
                    last_end_rel = windows[-1][1]
                else:
                    last_end_rel = 0  # 當日無棄電則把尾段視為從 0 開始（全靠電池）
                # 逐小時放電，直到沒電或到當日結束
                for rel_h in range(last_end_rel, L):
                    delivered = self.battery.discharge(
                        request_mw=self.battery.max_discharge_mw, hours=1.0
                    )
                    if delivered <= 1e-12:
                        break
                    tail_it_cap[rel_h] = delivered / pue  # MWh@1h -> MW

            # --- 4) 用「總 IT 容量 = 當下棄電 IT + 尾段 IT」再嘗試把『尚未排入』的作業塞進去 ---
            total_cap_it = cap_it_now + tail_it_cap
            # 只挑還沒排到的作業，且 duration 不超過當日剩餘小時數（簡單保守）
            remaining_jobs = [j for j in day_jobs if j.job_id not in scheduled_ids and j.duration_h <= L]
            # 再 pack 一次（一次性對整天），避免雙峰
            self._pack_nonpreemptive_blocks(
                jobs=remaining_jobs,
                used_it=used_it,
                cap_it=total_cap_it,
                day_start_abs=day_start,
                scheduled_ids=scheduled_ids,
            )

            # --- 5) 產生下一天的 backlog（可選） ---
            if carry_backlog:
                # 把還沒排到、而且「可能在明天還有機會（duration ≤ 24）」的留下
                backlog = [j for j in day_jobs if j.job_id not in scheduled_ids and j.duration_h <= 24]
            else:
                backlog = []

            # --- 6) 回寫當日 IT 需求 ---
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
            it, jobs_scheduled = self._schedule_only_curtail(jobs, curtailed, use_battery=use_battery)

        # Facility power = IT * PUE
        return it * float(self.config.pue), jobs_scheduled
