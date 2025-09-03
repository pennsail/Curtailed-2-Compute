import numpy as np
import pandas as pd
import pickle
from datacenter import DataCenter, DataCenterConfig
from battery import Battery
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import copy

# ---- Strategy knobs for "only_curtail" (透傳到 datacenter) ----
RESERVE_FRAC_FOR_BATT = 0.40   # 每個有棄電的小時預留 20% 功率給電池充電
CARRY_BACKLOG = True           # 前一天沒排到的作業帶到隔天

H = 168  # one week in hours
scale_jobs = True  # scale jobs to fit capacity
grid_case = 'high_curtailment'

# -------------------------
# Loaders (strict checking)
# -------------------------
def load_price_and_curtailment_data(H=168):
    curtail_df = pd.read_csv(f"vector_{grid_case}_week_v2.csv")
    required_cols = ["LMP_NP15", "Total_Curtailment_NP15_MW"]
    for c in required_cols:
        if c not in curtail_df.columns:
            raise ValueError(f"Missing column '{c}' in vector_{grid_case}_week_v2.csv")
    if len(curtail_df) < H:
        raise ValueError(f"curtailment/price file has only {len(curtail_df)} rows, need >= {H}")

    price_vector = curtail_df["LMP_NP15"].values[:H].astype(float)
    curtailed_supply = curtail_df["Total_Curtailment_NP15_MW"].values[:H].astype(float)

    if np.any(~np.isfinite(price_vector)) or np.any(~np.isfinite(curtailed_supply)):
        raise ValueError("NaN/inf found in price or curtailment vectors.")

    return price_vector, curtailed_supply


def load_carbon_vector(H=168):
    """Load CAISO hourly carbon intensity from CSV (lbs CO2/MWh -> kg CO2/MWh)."""
    curtail_df = pd.read_csv(f"vector_{grid_case}_week_v2.csv")
    if "marginal_co2_lbs_per_mwh" not in curtail_df.columns:
        raise ValueError("Missing column 'marginal_co2_lbs_per_mwh' in vector_{grid_case}_week_v2.csv")
    if len(curtail_df) < H:
        raise ValueError(f"carbon file has only {len(curtail_df)} rows, need >= {H}")

    # Convert lbs to kg (1 lb = 0.453592 kg)
    carbon_lbs = curtail_df["marginal_co2_lbs_per_mwh"].values[:H].astype(float)
    carbon_vector = carbon_lbs * 0.453592

    if np.any(~np.isfinite(carbon_vector)):
        raise ValueError("NaN/inf in carbon vector.")
    return carbon_vector


# -------------------------
# Helper: build/attach BESS
# -------------------------
def attach_battery(dc: DataCenter, battery_capacity_mw: float, duration_h: float = 4.0, rte: float = 0.92):
    if battery_capacity_mw <= 0:
        dc.battery = None
        return None
    batt = Battery(
        capacity_mwh=battery_capacity_mw * duration_h,
        max_charge_mw=battery_capacity_mw,
        max_discharge_mw=battery_capacity_mw,
        round_trip_efficiency=rte,
        soc_mwh=0.0,
    )
    dc.battery = batt
    return batt


# -------------------------
# Core analysis
# -------------------------
def analyze_strategy(
    strategy_name: str,
    dc: DataCenter,
    battery_capacity_mw: float,
    price_vector: np.ndarray,
    curtailed_supply: np.ndarray,
    carbon_vector: np.ndarray,
    bess_params: tuple[float, float, float],  # (duration_h, $/kWh, fire $/kW)
    job_scale_factor: float = 1.0,
):
    if price_vector.shape[0] != H or curtailed_supply.shape[0] != H or carbon_vector.shape[0] != H:
        raise ValueError("All vectors (price, curtailment, carbon) must be length 168.")

    duration_h, cost_per_kwh, fire_cost_per_kw = bess_params

    # 1) attach battery and run simulate
    batt = attach_battery(dc, battery_capacity_mw, duration_h=duration_h, rte=0.92)
    use_battery = batt is not None

    # 2) run simulate to get scheduling + energy accounting
    df = dc.simulate(
        strategy=strategy_name,
        use_battery=use_battery,
        curtailed_supply_mw=curtailed_supply,
        price_vector_per_mwh=price_vector,
        carbon_vector_kg_per_mwh=carbon_vector,
        reserve_frac_for_batt=RESERVE_FRAC_FOR_BATT if strategy_name == "only_curtail" else None,
        carry_backlog=CARRY_BACKLOG if strategy_name == "only_curtail" else None,
    )

    # 3) get results from simulation
    jobs_scheduled = df.attrs["totals"]["jobs_scheduled"]
    total_energy_mwh = df.attrs["totals"]["total_energy_mwh"]
    total_cost_usd = df.attrs["totals"]["total_cost_usd"]
    total_carbon_kg = df.attrs["totals"]["total_emissions_kg"]

    # Apply job scaling factor to match datacenter upscaling
    if scale_jobs:
        jobs_scheduled = int(jobs_scheduled * job_scale_factor)

    # Only OpEx: grid electricity cost (no battery CapEx)
    total_cost_opex = total_cost_usd  # This already includes grid electricity costs
    cost_per_job = total_cost_opex / max(jobs_scheduled, 1)
    carbon_per_job = total_carbon_kg / max(jobs_scheduled, 1)

    return {
        "strategy": strategy_name,
        "battery_capacity_mw": battery_capacity_mw,
        "total_jobs_scheduled": jobs_scheduled,
        "total_cost_usd": total_cost_opex,
        "total_carbon_kg": total_carbon_kg,
        "cost_per_job": cost_per_job,
        "carbon_per_job": carbon_per_job,
        "total_energy_mwh": total_energy_mwh,
    }


def run_analysis_task(task_args):
    """Worker function for parallel processing"""
    strategy, batt_mw, config, price_vector, curtailed_supply, carbon_vector, bess_params, job_scale_factor = task_args
    
    # Create fresh DataCenter instance for each task to avoid shared state issues
    dc = DataCenter(csv_path="/z/azure/vmtable.csv", config=config, scale_jobs=scale_jobs)
    
    return analyze_strategy(
        strategy_name=strategy,
        dc=dc,
        battery_capacity_mw=batt_mw,
        price_vector=price_vector,
        curtailed_supply=curtailed_supply,
        carbon_vector=carbon_vector,
        bess_params=bess_params,
        job_scale_factor=job_scale_factor,
    )

def main():
    # 1) inputs
    price_vector, curtailed_supply = load_price_and_curtailment_data(H)
    carbon_vector = load_carbon_vector(H)

    # 2) datacenter config
    config = DataCenterConfig(
        capacity_mw=20.0,
        pue=1.2,
        week_hours=H,
        timezone="UTC",
    )
    dc = DataCenter(csv_path="/z/azure/vmtable.csv", config=config, scale_jobs=scale_jobs)

    # 3) BESS economics
    BESS_DURATION_HOURS = 4.0
    BESS_COST_PER_KWH = 150.0
    BESS_FIRE_SUPPRESSION_COST_PER_KW = 8.0
    bess_params = (BESS_DURATION_HOURS, BESS_COST_PER_KWH, BESS_FIRE_SUPPRESSION_COST_PER_KW)

    # 4) scenarios
    battery_capacities = list(range(0, 41, 4))  # MW: [0, 4, 8, 12, 16, 20]
    strategies = ["as_is", "only_curtail", "carbon_aware"]
    
    # Get scaling factor for job count adjustment
    if dc._jobs is None:
        dc._extract_jobs_from_vms()
    if dc._hourly_it_mw_raw is None:
        dc._hourly_it_mw_raw = dc._build_hourly_it_from_jobs_default(dc._jobs or [])
    
    peak_raw = float(np.max(dc._hourly_it_mw_raw)) if dc._hourly_it_mw_raw.size else 1.0
    target = float(dc.config.capacity_mw)
    job_scale_factor = target / peak_raw if peak_raw > 0 else 1.0
    print(f"Job scaling factor to fit {target} MW capacity: {job_scale_factor:.3f}")

    # Create all combinations for parallel processing
    tasks = []
    for strategy in strategies:
        for batt_mw in battery_capacities:
            tasks.append((strategy, batt_mw, config, price_vector, curtailed_supply, carbon_vector, bess_params, job_scale_factor))
    
    print(f"\nRunning {len(tasks)} analysis tasks in parallel...")
    
    # Parallel execution
    with ProcessPoolExecutor(max_workers=min(len(tasks), 40)) as executor:
        results = list(executor.map(run_analysis_task, tasks))
    
    # Print results grouped by strategy
    for strategy in strategies:
        print(f"\n=== Strategy: {strategy} ===")
        strategy_results = [r for r in results if r['strategy'] == strategy]
        for res in sorted(strategy_results, key=lambda x: x['battery_capacity_mw']):
            print(f"  - Battery: {res['battery_capacity_mw']} MW")
            print(f"    Jobs scheduled:     {res['total_jobs_scheduled']:>7}")
            print(f"    Total energy (MWh): {res['total_energy_mwh']:.1f}")
            print(f"    Total OpEx:         ${res['total_cost_usd']:.0f}")
            print(f"    Cost per job:       ${res['cost_per_job']:.2f}")
            print(f"    Carbon per job:     {res['carbon_per_job']:.2f} kg")

    df = pd.DataFrame(results)
    # save to csv, name it depending on the input curtailment file
    df.to_csv(f"battery_analysis_results_{grid_case}.csv", index=False)
    print("\n=== Summary ===")
    print(df.to_string(index=False))
    print(f"\nResults saved to battery_analysis_results_{grid_case}.csv")


if __name__ == "__main__":
    main()
