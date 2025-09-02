import numpy as np
import pandas as pd
import pickle
from datacenter import DataCenter, DataCenterConfig
from battery import Battery
from datetime import datetime

# ---- Strategy knobs for "only_curtail" (透傳到 datacenter) ----
RESERVE_FRAC_FOR_BATT = 0.20   # 每個有棄電的小時預留 20% 功率給電池充電
CARRY_BACKLOG = True           # 前一天沒排到的作業帶到隔天

H = 168  # one week in hours


# -------------------------
# Loaders (strict checking)
# -------------------------
def load_price_and_curtailment_data(H=168):
    curtail_df = pd.read_csv("vector_high_curtailment_week.csv")
    required_cols = ["LMP_NP15", "Total_Curtailment_NP15_MW"]
    for c in required_cols:
        if c not in curtail_df.columns:
            raise ValueError(f"Missing column '{c}' in vector_high_curtailment_week.csv")
    if len(curtail_df) < H:
        raise ValueError(f"curtailment/price file has only {len(curtail_df)} rows, need >= {H}")

    price_vector = curtail_df["LMP_NP15"].values[:H].astype(float)
    curtailed_supply = curtail_df["Total_Curtailment_NP15_MW"].values[:H].astype(float)

    if np.any(~np.isfinite(price_vector)) or np.any(~np.isfinite(curtailed_supply)):
        raise ValueError("NaN/inf found in price or curtailment vectors.")

    return price_vector, curtailed_supply


def load_carbon_vector(H=168):
    """Load CAISO hourly carbon intensity (kg CO2/MWh). Accept 24h profile (tile to 168) or full 168."""
    caiso_ci_pkl = "caiso_carbon_intensity_2025.pkl"
    with open(caiso_ci_pkl, "rb") as f:
        ci = pickle.load(f)

    # 嚴格檢查索引
    if "CAISO" not in ci or "2025" not in ci["CAISO"] or "average" not in ci["CAISO"]["2025"]:
        raise ValueError("Expected ci['CAISO']['2025']['average'] in carbon intensity pickle.")

    avg_ci = np.array(ci["CAISO"]["2025"]["average"], dtype=float)
    if avg_ci.size not in (24, H):
        raise ValueError("Expected carbon intensity vector of length 24 or 168.")
    carbon_vector = (np.tile(avg_ci, 7) if avg_ci.size == 24 else avg_ci)[:H]

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
):
    if price_vector.shape[0] != H or curtailed_supply.shape[0] != H or carbon_vector.shape[0] != H:
        raise ValueError("All vectors (price, curtailment, carbon) must be length 168.")

    duration_h, cost_per_kwh, fire_cost_per_kw = bess_params

    # 1) attach battery for SCHEDULING
    batt = attach_battery(dc, battery_capacity_mw, duration_h=duration_h, rte=0.92)
    use_battery = batt is not None

    # 2) build schedule (returns facility MW and job count)
    if strategy_name == "as_is":
        demand_mw, jobs_scheduled = dc.demand_facility_mw(
            strategy="as_is",
            use_battery=False,  # 調度不吃電池（電池影響放到 simulate 裡）
            curtailed_supply_mw=curtailed_supply,
            price_vector_per_mwh=price_vector,
            carbon_vector_kg_per_mwh=carbon_vector,
        )
    elif strategy_name == "only_curtail":
        demand_mw, jobs_scheduled = dc.demand_facility_mw(
            strategy="only_curtail",
            use_battery=use_battery,
            curtailed_supply_mw=curtailed_supply,
            price_vector_per_mwh=price_vector,
            carbon_vector_kg_per_mwh=carbon_vector,
        )
    elif strategy_name == "carbon_aware":
        demand_mw, jobs_scheduled = dc.demand_facility_mw(
            strategy="carbon_aware",
            use_battery=use_battery,  # 電池只影響後續能量會計
            curtailed_supply_mw=curtailed_supply,
            price_vector_per_mwh=price_vector,
            carbon_vector_kg_per_mwh=carbon_vector,
        )
    else:
        raise ValueError(f"Unknown strategy: {strategy_name}")

    if demand_mw.shape[0] != H:
        raise ValueError("DataCenter returned wrong-length demand vector.")

    # 3) re-attach a FRESH battery for ENERGY ACCOUNTING (避免調度階段改變了 SOC)
    batt = attach_battery(dc, battery_capacity_mw, duration_h=duration_h, rte=0.92)
    use_battery = batt is not None

    # 4) run simulate to get grid-vs-curtail split and cost
    only_curtail_flag = (strategy_name == "only_curtail")
    carbon_responder_flag = (strategy_name == "carbon_aware")

    # 注意：我們假定 datacenter.simulate 會把下列 kwargs 透傳給 demand_facility_mw，
    # 以保證 simulate 內部採用與上面相同的調度策略與參數。
    df = dc.simulate(
        strategy=strategy_name,
        use_battery=use_battery,
        curtailed_supply_mw=curtailed_supply,
        price_vector_per_mwh=price_vector,
        carbon_vector_kg_per_mwh=carbon_vector,
        # pass the two curtailment knobs through (only used by only_curtail)
        # reserve_frac_for_batt=RESERVE_FRAC_FOR_BATT if strategy_name == "only_curtail" else None,
        # carry_backlog=CARRY_BACKLOG if strategy_name == "only_curtail" else None,
        reset_battery_soc=True,
    )

    # 5) totals：成本直接用 simulate（含電池充放的能量分解與價格），
    #    碳排用逐時電網能量 * 逐時碳強度（更精確）
    if "met_by_grid_mw" not in df.columns:
        raise RuntimeError("simulate() did not return 'met_by_grid_mw' column.")

    grid_mwh = df["met_by_grid_mw"].to_numpy(dtype=float)  # 每小時等於 MW（1h）
    curtailed_mwh = df["met_by_curtail_mw"].to_numpy(dtype=float)
    total_energy_mwh = float(df["demand_mw"].sum())

    # 成本直接採用 simulate 的總成本
    totals = df.attrs.get("totals", {})
    total_cost_usd = float(totals.get("total_cost_usd", float(np.sum(demand_mw * price_vector))))

    # 逐時碳排：網電 * carbon_vector；棄電 * 0（或另給值）
    curtailed_ci = 0.0
    total_carbon_kg = float(np.dot(grid_mwh, carbon_vector) + np.sum(curtailed_mwh) * curtailed_ci)

    # 加上 BESS 資本支出
    battery_capex = 0.0
    if battery_capacity_mw > 0:
        battery_energy_kwh = battery_capacity_mw * 1000 * duration_h
        battery_power_kw = battery_capacity_mw * 1000
        battery_capex = battery_energy_kwh * cost_per_kwh + battery_power_kw * fire_cost_per_kw

    total_cost_with_bess = total_cost_usd + battery_capex
    cost_per_job = total_cost_with_bess / max(jobs_scheduled, 1)
    carbon_per_job = total_carbon_kg / max(jobs_scheduled, 1)

    return {
        "strategy": strategy_name,
        "battery_capacity_mw": battery_capacity_mw,
        "battery_capex_usd": battery_capex,
        "total_jobs_scheduled": int(jobs_scheduled),
        "total_cost_usd": total_cost_with_bess,
        "total_carbon_kg": total_carbon_kg,
        "cost_per_job": cost_per_job,
        "carbon_per_job": carbon_per_job,
        "total_energy_mwh": total_energy_mwh,
    }


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
    dc = DataCenter(csv_path="/z/azure/vmtable.csv", config=config, scale_jobs=True)

    # 3) BESS economics
    BESS_DURATION_HOURS = 4.0
    BESS_COST_PER_KWH = 150.0
    BESS_FIRE_SUPPRESSION_COST_PER_KW = 8.0
    bess_params = (BESS_DURATION_HOURS, BESS_COST_PER_KWH, BESS_FIRE_SUPPRESSION_COST_PER_KW)

    # 4) scenarios
    battery_capacities = [0, 5, 10, 20]  # MW
    strategies = ["as_is", "only_curtail", "carbon_aware"]

    results = []
    for strategy in strategies:
        print(f"\n=== Strategy: {strategy} ===")
        for batt_mw in battery_capacities:
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
            print(f"    Jobs scheduled:     {res['total_jobs_scheduled']:>7}")
            print(f"    Battery CapEx:      ${res['battery_capex_usd']:.0f}")
            print(f"    Total energy (MWh): {res['total_energy_mwh']:.1f}")
            print(f"    Total cost:         ${res['total_cost_usd']:.0f}")
            print(f"    Cost per job:       ${res['cost_per_job']:.2f}")
            print(f"    Carbon per job:     {res['carbon_per_job']:.2f} kg")

    df = pd.DataFrame(results)
    df.to_csv("battery_analysis_results.csv", index=False)
    print("\n=== Summary ===")
    print(df.to_string(index=False))
    print("\nResults saved to battery_analysis_results.csv")


if __name__ == "__main__":
    main()
