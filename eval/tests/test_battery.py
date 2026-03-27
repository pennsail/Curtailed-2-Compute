"""Tests for the Battery model."""

import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from battery import Battery


# --------------- Construction ---------------

def test_default_construction():
    b = Battery(capacity_mwh=40, max_charge_mw=10, max_discharge_mw=10)
    assert b.soc_mwh == 0.0
    assert b.round_trip_efficiency == 0.92


def test_invalid_efficiency_raises():
    with pytest.raises(ValueError):
        Battery(capacity_mwh=40, max_charge_mw=10, max_discharge_mw=10,
                round_trip_efficiency=1.5)


# --------------- Charge ---------------

def test_charge_respects_power_limit():
    b = Battery(capacity_mwh=100, max_charge_mw=5, max_discharge_mw=5, soc_mwh=0)
    drawn = b.charge(request_mw=20, hours=1.0)  # request exceeds max
    assert drawn <= 5.0 + 1e-9


def test_charge_respects_capacity():
    b = Battery(capacity_mwh=10, max_charge_mw=100, max_discharge_mw=100, soc_mwh=9.5)
    b.charge(request_mw=100, hours=1.0)
    assert b.soc_mwh <= b.capacity_mwh + 1e-9


def test_charge_zero_request():
    b = Battery(capacity_mwh=40, max_charge_mw=10, max_discharge_mw=10, soc_mwh=5)
    drawn = b.charge(request_mw=0, hours=1.0)
    assert drawn == 0.0
    assert b.soc_mwh == 5.0


# --------------- Discharge ---------------

def test_discharge_respects_power_limit():
    b = Battery(capacity_mwh=100, max_charge_mw=10, max_discharge_mw=5, soc_mwh=100)
    delivered = b.discharge(request_mw=20, hours=1.0)
    assert delivered <= 5.0 + 1e-9


def test_discharge_respects_soc():
    b = Battery(capacity_mwh=100, max_charge_mw=10, max_discharge_mw=10, soc_mwh=1.0)
    delivered = b.discharge(request_mw=10, hours=1.0)
    # Can't deliver more energy than stored (times efficiency)
    assert delivered <= 1.0 * b._eta_d + 1e-9
    assert b.soc_mwh >= 0.0


def test_discharge_empty_battery():
    b = Battery(capacity_mwh=40, max_charge_mw=10, max_discharge_mw=10, soc_mwh=0)
    delivered = b.discharge(request_mw=5, hours=1.0)
    assert delivered == 0.0


# --------------- Round-trip efficiency ---------------

def test_round_trip_energy_conservation():
    """Charge then discharge: energy out <= energy in (losses from RTE)."""
    b = Battery(capacity_mwh=100, max_charge_mw=50, max_discharge_mw=50,
                round_trip_efficiency=0.90, soc_mwh=0)
    energy_in = b.charge(request_mw=10, hours=1.0)
    energy_out = b.discharge(request_mw=50, hours=1.0)
    assert energy_out < energy_in
    assert energy_out == pytest.approx(energy_in * 0.90, rel=0.01)


# --------------- Snapshot / Restore ---------------

def test_snapshot_restore():
    b = Battery(capacity_mwh=40, max_charge_mw=10, max_discharge_mw=10, soc_mwh=20)
    snap = b.snapshot()
    b.discharge(request_mw=10, hours=1.0)
    assert b.soc_mwh < 20.0
    b.restore(snap)
    assert b.soc_mwh == 20.0


def test_restore_clamps():
    b = Battery(capacity_mwh=40, max_charge_mw=10, max_discharge_mw=10)
    b.restore(999.0)
    assert b.soc_mwh == 40.0
    b.restore(-10.0)
    assert b.soc_mwh == 0.0


# --------------- SOC percentage ---------------

def test_soc_pct():
    b = Battery(capacity_mwh=40, max_charge_mw=10, max_discharge_mw=10, soc_mwh=20)
    assert b.soc_pct == pytest.approx(50.0)


def test_soc_pct_zero_capacity():
    b = Battery(capacity_mwh=0, max_charge_mw=0, max_discharge_mw=0, soc_mwh=0)
    assert b.soc_pct == 0.0
