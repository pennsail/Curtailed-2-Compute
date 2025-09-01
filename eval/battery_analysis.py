import numpy as np
import pandas as pd
import matplotlib.pyplot as plt  # 若不畫圖也可移除
import pickle
from datacenter import DataCenter, DataCenterConfig
from battery import Battery
from datetime import datetime

def load_price_and_curtailment_data(H=168):
    """Load LMP and curtailment data (first 168h)."""
    curtail_df = pd.read_csv("vector_high_curtailment_week.csv")
    price_vector = curtail_df['LMP_NP15'].values[:H].astype(float)
    curtailed_supply = curtail_df['Total_Curtailment_NP15_MW'].values[:H].astype(float)
    return price_vector, curtailed_supply

def load_carbon_vector(H=168):
    """Load CAISO carbon intensity vector (kg CO2/MWh).
       Accept 24h (day-average) or full 168h; tile if needed.
    """
    caiso_ci_pkl = "caiso_carbon_intensity_2025.pkl"
    print("Loading carbon intensity week vector...")
    with open(caiso_ci_pkl, "rb") as f:
        ci = pickle.load(f)
    avg_ci_24h = np.array(ci["CAISO"]["2025"]["average"], dtype=float)
    if avg_ci_24h.size not in (24, H):
        raise ValueError("Expected carbon intensity vector of length 24 (one day) or 168 (one week).")
    carbon_vector = (np.tile(avg_ci_24h, 7) if avg_ci_24h.size == 24 else avg_ci_24h)[:H]
    return carbon_vector

def analyze_strategy(
    strategy_name: str,
    dc: DataCenter,
    battery_capacity_mw: float,
    price_vector: np.ndarray,
    curtailed_supply: np.ndarray,
    carbon_vector: np.ndarray,
    bess_params: tuple,
):
    """Analyze a single strategy with given battery capacity."""
    H = dc.config.week_hours
    BESS_DURATION_HOURS, BESS_COST_PER_KWH, BESS_FIRE_SUPPRESSION_COST_PER_KW = bess_params

    # attach / detach battery
    if battery_capacity_mw > 0:
        dc.battery = Battery(
            capacity_mwh=battery_capacity_mw * BESS_DURATION_HOURS,
            max_charge_mw=battery_capacity_mw,
            max_discharge_mw=battery_capacity_mw,
            round_trip_efficiency=0.92,
            soc_mwh=0.0
        )
        use_battery = True
    else:
        dc.battery = None
        use_battery = False

    # build demand (MW) one week, using datacenter job scheduler
    if strategy_name == "as_is":
        demand_mw = dc.demand_facility_mw(
            strategy="as_is",
            use_battery=False,                   # as_is 調度不吃電池（電池影響成本/碳請改用 simulate，如需）
            curtailed_supply_mw=curtailed_supply,
            price_vector_per_mwh=price_vector,
            carbon_vector_kg_per_mwh=carbon_vector,
        )
    elif strategy_name == "curtail_only":
        demand_mw = dc.demand_facility_mw(
            strategy="only_curtail",
            use_battery=use_battery,             # 電池會延長窗
            curtailed_supply_mw=curtailed_supply,
            price_vector_per_mwh=price_vector,
            carbon_vector_kg_per_mwh=carbon_vector,
        )
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")

    # --- metrics (簡化：僅用 demand 與 price/carbon 向量逐時點乘) ---
    total_energy_mwh = float(np.sum(demand_mw))            # facility MWh
    total_cost = float(np.sum(demand_mw * price_vector))   # $ (簡化：假設所有能量按 price_vector 計價)
    total_carbon = float(np.sum(demand_mw * carbon_vector))# kg CO2

    # battery capex（若有）
    battery_capex = 0.0
    if battery_capacity_mw > 0:
        battery_energy_kwh = battery_capacity_mw * 1000 * BESS_DURATION_HOURS
        battery_power_kw  = battery_capacity_mw * 1000
        battery_capex = battery_energy_kwh * BESS_COST_PER_KWH + battery_power_kw * BESS_FIRE_SUPPRESSION_COST_PER_KW

    # job 計數（簡化估計）
    if dc._jobs is None:
        dc._extract_jobs_from_vms()
    all_jobs = dc._get_scaled_jobs() if dc.scale_jobs else dc._jobs

    if strategy_name == "as_is":
        total_jobs_scheduled = len(all_jobs)
    else:
        # 以 IT 能量比例估算被排入的 job 數（粗略）
        if len(all_jobs) == 0:
            total_jobs_scheduled = 0
        else:
            avg_job_it_mwh = float(np.mean([j.it_power_mw * j.duration_h for j in all_jobs]))
            it_energy_curtail = total_energy_mwh / dc.config.pue  # 反推 IT 能量
            denom = max(avg_job_it_mwh, 1e-9)
            total_jobs_scheduled = int(min(len(all_jobs), round(it_energy_curtail / denom)))

    total_cost_with_bess = total_cost + battery_capex
    cost_per_job = total_cost_with_bess / max(total_jobs_scheduled, 1)
    carbon_per_job = total_carbon / max(total_jobs_scheduled, 1)

    return {
        'strategy': strategy_name,
        'battery_capacity_mw': battery_capacity_mw,
        'battery_capex_usd': battery_capex,
        'total_jobs_scheduled': total_jobs_scheduled,
        'total_cost_usd': total_cost_with_bess,
        'total_carbon_kg': total_carbon,
        'cost_per_job': cost_per_job,
        'carbon_per_job': carbon_per_job,
        'total_energy_mwh': total_energy_mwh
    }

def main():
    H = 168
    price_vector, curtailed_supply = load_price_and_curtailment_data(H)
    carbon_vector = load_carbon_vector(H)

    # datacenter config
    config = DataCenterConfig(
        capacity_mw=20.0, 
        pue=1.2, 
        week_hours=H,
        timezone='UTC'
    )
    dc = DataCenter(csv_path="/z/azure/vmtable.csv", config=config, scale_jobs=True)

    # BESS parameters
    BESS_DURATION_HOURS = 4
    BESS_COST_PER_KWH = 150.0
    BESS_FIRE_SUPPRESSION_COST_PER_KW = 8.0
    bess_params = (BESS_DURATION_HOURS, BESS_COST_PER_KWH, BESS_FIRE_SUPPRESSION_COST_PER_KW)

    battery_capacities = [0, 5, 10, 20]  # MW
    strategies = ["as_is", "curtail_only"]

    results = []
    for strategy in strategies:
        print(f"\n=== Strategy: {strategy} ===")
        for batt_mw in battery_capacities:
            try:
                res = analyze_strategy(
                    strategy_name=strategy,
                    dc=dc,
                    battery_capacity_mw=batt_mw,
                    price_vector=price_vector,
                    curtailed_supply=curtailed_supply,
                    carbon_vector=carbon_vector,
                    bess_params=bess_params,
                )
                results.append(res)
                print(f"  - Battery: {batt_mw} MW")
                print(f"    Jobs scheduled:     {res['total_jobs_scheduled']:>6}")
                print(f"    Battery CapEx:      ${res['battery_capex_usd']:.0f}")
                print(f"    Total energy (MWh): {res['total_energy_mwh']:.1f}")
                print(f"    Total cost:         ${res['total_cost_usd']:.0f}")
                print(f"    Cost per job:       ${res['cost_per_job']:.2f}")
                print(f"    Carbon per job:     {res['carbon_per_job']:.2f} kg")
            except Exception as e:
                import traceback
                print(f"  - Battery: {batt_mw} MW")
                print(f"    ERROR: {e}")
                print(traceback.format_exc())

    # save
    df = pd.DataFrame(results)
    df.to_csv("battery_analysis_results.csv", index=False)
    print("\n=== Summary ===")
    print(df.to_string(index=False))
    print("\nResults saved to battery_analysis_results.csv")

if __name__ == "__main__":
    main()
