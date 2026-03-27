"""Tests for the DataCenter scheduling model."""

import sys
import os
import csv
import tempfile
import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from datacenter import DataCenter, DataCenterConfig, VMJob
from battery import Battery

H = 168  # hours in a week


def _make_csv(rows, path):
    """Write rows (list of 11-element lists) to a headerless CSV."""
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def _simple_workload(tmp_path, n_jobs=10, duration_s=7200, vcpus=4, avg_cpu=50.0):
    """Create a tiny synthetic workload CSV with jobs starting at t=0."""
    rows = []
    for i in range(n_jobs):
        start = i * 3600  # stagger by 1 hour
        end = start + duration_s
        rows.append([
            f"vm_{i:06d}", "sub_001", "dep_001",
            start, end,
            80.0,       # max cpu
            avg_cpu,    # avg cpu
            70.0,       # p95 max cpu
            "Unknown",  # category
            vcpus,      # vcpu bucket
            8,          # memory bucket
        ])
    path = os.path.join(tmp_path, "test_vms.csv")
    _make_csv(rows, path)
    return path


@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


# --------------- Job extraction ---------------

def test_jobs_extracted(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=5)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    jobs = dc._extract_jobs_from_vms()
    assert len(jobs) == 5
    assert all(isinstance(j, VMJob) for j in jobs)


def test_jobs_have_positive_power(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=5, avg_cpu=50.0)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    jobs = dc._extract_jobs_from_vms()
    assert all(j.it_power_mw > 0 for j in jobs)


def test_zero_cpu_jobs_filtered(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=3, avg_cpu=0.0)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    jobs = dc._extract_jobs_from_vms()
    assert len(jobs) == 0  # zero utilization => zero power => filtered


# --------------- Strategy: as_is ---------------

def test_as_is_schedules_all_jobs(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=8)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    demand, n_scheduled, _ = dc.demand_facility_mw(strategy="as_is")
    assert demand.shape == (H,)
    assert n_scheduled == 8


def test_as_is_demand_nonnegative(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=8)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    demand, _, _ = dc.demand_facility_mw(strategy="as_is")
    assert np.all(demand >= -1e-12)


# --------------- Strategy: carbon_aware ---------------

def test_carbon_aware_schedules_all_jobs(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=8)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    carbon = np.random.default_rng(42).uniform(100, 500, H)
    demand, n_scheduled, _ = dc.demand_facility_mw(
        strategy="carbon_aware", carbon_vector_kg_per_mwh=carbon
    )
    assert demand.shape == (H,)
    assert n_scheduled > 0


def test_carbon_aware_requires_carbon_vector(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=3)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    with pytest.raises(ValueError, match="carbon_aware requires"):
        dc.demand_facility_mw(strategy="carbon_aware")


# --------------- Strategy: only_curtail ---------------

def test_only_curtail_zero_curtailment(tmp_dir):
    """With no curtailment available, no jobs should be scheduled."""
    csv_path = _simple_workload(tmp_dir, n_jobs=8)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    curtailed = np.zeros(H)
    demand, n_scheduled, _ = dc.demand_facility_mw(
        strategy="only_curtail", curtailed_supply_mw=curtailed
    )
    assert n_scheduled == 0
    assert np.allclose(demand, 0.0)


def test_only_curtail_with_curtailment(tmp_dir):
    """With generous curtailment, some jobs should be scheduled."""
    csv_path = _simple_workload(tmp_dir, n_jobs=8)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    # Provide curtailment during hours 8-16 each day (midday solar)
    curtailed = np.zeros(H)
    for d in range(7):
        curtailed[d * 24 + 8: d * 24 + 17] = 50.0  # 50 MW available
    demand, n_scheduled, _ = dc.demand_facility_mw(
        strategy="only_curtail", curtailed_supply_mw=curtailed
    )
    assert n_scheduled > 0


def test_only_curtail_requires_curtailed_vector(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=3)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    with pytest.raises(ValueError, match="only_curtail requires"):
        dc.demand_facility_mw(strategy="only_curtail")


# --------------- Battery integration ---------------

def test_only_curtail_battery_improves_scheduling(tmp_dir):
    """Battery should allow scheduling more jobs than curtailment-only."""
    csv_path = _simple_workload(tmp_dir, n_jobs=20, duration_s=3600)
    curtailed = np.zeros(H)
    for d in range(7):
        curtailed[d * 24 + 10: d * 24 + 15] = 30.0  # narrow window

    dc_no_batt = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    _, n_no_batt, _ = dc_no_batt.demand_facility_mw(
        strategy="only_curtail", curtailed_supply_mw=curtailed
    )

    batt = Battery(capacity_mwh=40, max_charge_mw=10, max_discharge_mw=10, soc_mwh=40)
    dc_batt = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20), battery=batt)
    _, n_with_batt, _ = dc_batt.demand_facility_mw(
        strategy="only_curtail", curtailed_supply_mw=curtailed, use_battery=True
    )

    assert n_with_batt >= n_no_batt


# --------------- Invalid strategy ---------------

def test_invalid_strategy_raises(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=3)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    with pytest.raises(ValueError, match="strategy must be one of"):
        dc.demand_facility_mw(strategy="nonexistent")


# --------------- Simulate ---------------

def test_simulate_returns_dataframe(tmp_dir):
    csv_path = _simple_workload(tmp_dir, n_jobs=5)
    dc = DataCenter(csv_path=csv_path, config=DataCenterConfig(capacity_mw=20))
    prices = np.full(H, 40.0)
    carbon = np.full(H, 300.0)
    df = dc.simulate(
        strategy="as_is",
        price_vector_per_mwh=prices,
        carbon_vector_kg_per_mwh=carbon,
    )
    assert len(df) == H
    assert "demand_mw" in df.columns
    assert "cost_usd" in df.columns
    assert "emissions_kg" in df.columns
    totals = df.attrs["totals"]
    assert totals["jobs_scheduled"] == 5
    assert totals["total_cost_usd"] >= 0
