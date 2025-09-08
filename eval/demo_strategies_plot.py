# demo_strategies_plot.py
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pickle
from pathlib import Path
from datetime import datetime

from datacenter import DataCenter, DataCenterConfig

def main():
    # -----------------------------
    # Inputs & basic config
    # -----------------------------
    H = 7 * 24
    hourly_power_path = Path("hourly_power.npy")  # optional (原始 trace 的每小時功率，單位 W)
    vmtable_csv = "/z/azure/vmtable.csv"         # DataCenter 將用此 CSV 產生作業
    grid_case = 'high_curtailment'
    curtail_csv = f"vector_{grid_case}_week_v2.csv"

    datacenter_total_capacity_mw = 20.0  # Facility cap (e.g., nameplate)
    pue = 1.2
    datacenter_it_capacity_mw = datacenter_total_capacity_mw / pue

    # -----------------------------
    # Load vectors (curtail & carbon)
    # -----------------------------
    print("Loading curtailed power and carbon intensity week vector...")
    curtail_df = pd.read_csv(curtail_csv)
    
    # Load curtailment data
    if "Total_Curtailment_NP15_MW" not in curtail_df.columns:
        raise ValueError("Missing column 'Total_Curtailment_NP15_MW' in data file")
    curtailed_supply = curtail_df["Total_Curtailment_NP15_MW"].to_numpy()
    if len(curtailed_supply) < H:
        raise ValueError(f"curtailment vector length={len(curtailed_supply)} < {H}.")
    curtailed_supply = curtailed_supply[:H].astype(float)
    
    # Load carbon intensity data (convert lbs to kg)
    if "marginal_co2_lbs_per_mwh" not in curtail_df.columns:
        raise ValueError("Missing column 'marginal_co2_lbs_per_mwh' in data file")
    carbon_lbs = curtail_df["marginal_co2_lbs_per_mwh"].to_numpy()
    if len(carbon_lbs) < H:
        raise ValueError(f"carbon vector length={len(carbon_lbs)} < {H}.")
    # Convert lbs to kg (1 lb = 0.453592 kg)
    carbon_intensity_week = (carbon_lbs[:H] * 0.453592).astype(float)

    # -----------------------------
    # Optional: original hourly power (unmodified)
    # -----------------------------
    if hourly_power_path.exists():
        print("Loading analyzed VM data (hourly_power.npy)...")
        hourly_power_w = np.load(hourly_power_path)
        if hourly_power_w.size < H:
            raise ValueError(f"hourly_power.npy length={hourly_power_w.size} < {H}.")
        raw_demand_mw = (hourly_power_w[:H] / 1e6).astype(float)
    else:
        print("[WARN] hourly_power.npy not found. Using zeros as placeholder for Plot 1.")
        raw_demand_mw = np.zeros(H, dtype=float)

    # -----------------------------
    # Build DataCenter
    # -----------------------------
    config = DataCenterConfig(
        capacity_mw=datacenter_it_capacity_mw,  # IT capacity
        pue=pue,
        watts_per_vcpu=20.0,
        utilization_column="avg cpu",
        week_hours=H,
        timezone="UTC",
    )
    dc = DataCenter(csv_path=vmtable_csv, config=config, scale_jobs=True)

    # -----------------------------
    # Parallel scenario generation
    # -----------------------------
    from concurrent.futures import ThreadPoolExecutor
    
    def generate_scenario_a():
        print("Generating Scenario A (as-is, capacity-constrained)...")
        return dc.demand_facility_mw(strategy="as_is", use_battery=False)
    
    def generate_scenario_b():
        print("Generating Scenario B (curtailment-only)...")
        return dc.demand_facility_mw(
            strategy="only_curtail",
            use_battery=False,
            curtailed_supply_mw=curtailed_supply
        )
    
    def generate_scenario_c():
        print("Generating Scenario C (carbon-aware)...")
        return dc.demand_facility_mw(
            strategy="carbon_aware",
            use_battery=False,
            carbon_vector_kg_per_mwh=carbon_intensity_week
        )
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            'as_is': executor.submit(generate_scenario_a),
            'curtail': executor.submit(generate_scenario_b),
            'carbon': executor.submit(generate_scenario_c)
        }
        
        demand_as_is_fac_mw, _, _ = futures['as_is'].result()
        demand_curtail_fac_mw, _, _ = futures['curtail'].result()
        demand_carbon_fac_mw, _, _ = futures['carbon'].result()

    # -----------------------------
    # Plotting
    # -----------------------------
    print("Creating comparison plots...")
    hours = np.arange(H)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6, 6))

    # Plot 1: Price and Carbon Intensity
    # Load price data
    if "LMP_NP15" in curtail_df.columns:
        prices = curtail_df["LMP_NP15"].to_numpy()[:H]
    else:
        prices = np.random.uniform(30, 150, H)  # Fallback if no price data
    
    ax1.plot(hours, prices, linewidth=1.5, label='Electricity Price', color='blue')
    ax1.set_title('Plot 1: Electricity Price and Carbon Intensity', fontsize=14)
    ax1.set_xlabel('Hour')
    ax1.set_ylabel('Price ($/MWh)', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(True, alpha=0.3)

    # Carbon intensity as secondary axis
    ax1_r = ax1.twinx()
    ax1_r.plot(hours, carbon_intensity_week, linestyle='--', alpha=0.9, label='Carbon intensity', color='red')
    ax1_r.set_ylabel('Carbon Intensity (kg CO₂/MWh)', color='red')
    ax1_r.tick_params(axis='y', labelcolor='red')
    # Legends
    ln1, lb1 = ax1.get_legend_handles_labels()
    ln2, lb2 = ax1_r.get_legend_handles_labels()
    ax1.legend(ln1+ln2, lb1+lb2, loc='lower right', framealpha=1.0)

    # Day markers
    for d in range(8):
        ax1.axvline(d*24, color='gray', linestyle=':', alpha=0.35)

    # Plot 2: DC strategies
    ax2.plot(hours, demand_as_is_fac_mw, linewidth=1.5, label='a) Jobs as-is (no scheduling)')
    ax2.plot(hours, demand_curtail_fac_mw, linewidth=1.5, label='b) Curtailment-only')
    ax2.plot(hours, demand_carbon_fac_mw, linewidth=1.5, label='c) Carbon-aware')
    ax2.fill_between(hours, 0, curtailed_supply, alpha=0.3, color='gray', label='Available curtailed (facility MW)')

    ax2.set_title('Plot 2: Datacenter Scheduling Strategies (Facility MW)', fontsize=14)
    ax2.set_xlabel('Hour')
    ax2.set_ylabel('Power (MW)')
    ax2.set_ylim(0, datacenter_total_capacity_mw * 1.1)
    ax2.grid(True, alpha=0.3)
    ax2.legend()

    for d in range(8):
        ax2.axvline(d*24, color='gray', linestyle=':', alpha=0.35)

    plt.tight_layout()
    plt.savefig(f'power_usage_comparison_{grid_case}.pdf', dpi=150, bbox_inches='tight')
    print(f"Saved: power_usage_comparison_{grid_case}.pdf")

    # -----------------------------
    # Summary statistics
    # -----------------------------
    def stats(label, series):
        return (
            f"{label}\n"
            f"  Peak:  {series.max():.2f} MW\n"
            f"  Mean:  {series.mean():.2f} MW\n"
            f"  Energy:{series.sum():.1f} MWh\n"
        )

    print("\n=== Summary ===")
    if hourly_power_path.exists():
        print(stats("Original (unmodified, facility-equiv.)", raw_demand_mw))
    print(stats("Scenario A - as-is (scaled to cap)", demand_as_is_fac_mw))
    print(stats("Scenario B - curtailment-only", demand_curtail_fac_mw))
    print(stats("Scenario C - carbon-aware", demand_carbon_fac_mw))

    # 粗略的「碳」對比（僅示意：以 facility MWh × 小時碳強度）
    ca_emissions_kg = float(np.sum(demand_carbon_fac_mw * carbon_intensity_week))
    print(f"Carbon-aware (rough) emissions over week: {ca_emissions_kg:,.0f} kg CO₂")

    # 利用率 vs 可用棄電
    curtail_used_mwh = float(np.minimum(demand_curtail_fac_mw, curtailed_supply).sum())
    curtail_avail_mwh = float(curtailed_supply.sum())
    util_pct = (curtail_used_mwh / curtail_avail_mwh * 100.0) if curtail_avail_mwh > 1e-9 else 0.0
    print(f"Curtailment utilization: used {curtail_used_mwh:.1f} / {curtail_avail_mwh:.1f} MWh ({util_pct:.1f}%)")

if __name__ == "__main__":
    main()
