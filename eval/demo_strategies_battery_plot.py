# demo_strategies_battery_plot.py
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from datacenter import DataCenter, DataCenterConfig
from battery import Battery

def main():
    # -----------------------------
    # Inputs & basic config
    # -----------------------------
    H = 7 * 24
    hourly_power_path = Path("hourly_power.npy")
    vmtable_csv = "/z/azure/vmtable.csv"
    grid_case = 'high_curtailment'
    curtail_csv = f"vector_{grid_case}_week_v2.csv"

    datacenter_total_capacity_mw = 20.0
    pue = 1.2
    datacenter_it_capacity_mw = datacenter_total_capacity_mw / pue
    
    # Battery configuration
    battery_capacity_mw = 10.0
    battery_duration_hours = 4.0

    # -----------------------------
    # Load vectors (curtail & carbon)
    # -----------------------------
    print("Loading curtailed power and carbon intensity week vector...")
    curtail_df = pd.read_csv(curtail_csv)
    
    curtailed_supply = curtail_df["Total_Curtailment_NP15_MW"].to_numpy()[:H].astype(float)
    carbon_lbs = curtail_df["marginal_co2_lbs_per_mwh"].to_numpy()
    carbon_intensity_week = (carbon_lbs[:H] * 0.453592).astype(float)

    # -----------------------------
    # Build DataCenter with Battery
    # -----------------------------
    config = DataCenterConfig(
        capacity_mw=datacenter_it_capacity_mw,
        pue=pue,
        watts_per_vcpu=20.0,
        utilization_column="avg cpu",
        week_hours=H,
        timezone="UTC",
    )
    dc = DataCenter(csv_path=vmtable_csv, config=config, scale_jobs=True)
    
    # Add battery
    battery = Battery(
        capacity_mwh=battery_capacity_mw * battery_duration_hours,
        max_charge_mw=battery_capacity_mw,
        max_discharge_mw=battery_capacity_mw,
        round_trip_efficiency=0.9
    )
    dc.battery = battery

    # -----------------------------
    # Parallel scenario generation with battery
    # -----------------------------
    def generate_scenario_a():
        print("Generating Scenario A (as-is with battery)...")
        return dc.demand_facility_mw(strategy="as_is", use_battery=True)
    
    def generate_scenario_b():
        print("Generating Scenario B (curtailment-only with battery)...")
        return dc.demand_facility_mw(
            strategy="only_curtail",
            use_battery=True,
            curtailed_supply_mw=curtailed_supply
        )
    
    def generate_scenario_c():
        print("Generating Scenario C (carbon-aware with battery)...")
        return dc.demand_facility_mw(
            strategy="carbon_aware",
            use_battery=True,
            carbon_vector_kg_per_mwh=carbon_intensity_week
        )
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            'as_is': executor.submit(generate_scenario_a),
            'curtail': executor.submit(generate_scenario_b),
            'carbon': executor.submit(generate_scenario_c)
        }
        
        demand_as_is_fac_mw, _, battery_as_is = futures['as_is'].result()
        demand_curtail_fac_mw, _, battery_curtail = futures['curtail'].result()
        demand_carbon_fac_mw, _, battery_carbon = futures['carbon'].result()

    # -----------------------------
    # Plotting (3x1 subplots)
    # -----------------------------
    print("Creating comparison plots...")
    hours = np.arange(H)

    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(6, 9))

    # Plot 1: Price and Carbon Intensity
    if "LMP_NP15" in curtail_df.columns:
        prices = curtail_df["LMP_NP15"].to_numpy()[:H]
    else:
        prices = np.random.uniform(30, 150, H)
    
    ax1.plot(hours, prices, linewidth=1.5, label='Electricity Price', color='blue')
    ax1.set_title('Electricity Price and Carbon Intensity', fontsize=14)
    ax1.set_xlabel('Hour')
    ax1.set_ylabel('Price ($/MWh)', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(True, alpha=0.3)

    ax1_r = ax1.twinx()
    ax1_r.plot(hours, carbon_intensity_week, linestyle='--', alpha=0.9, label='Carbon intensity', color='red')
    ax1_r.set_ylabel('Carbon Intensity (kg CO₂/MWh)', color='red')
    ax1_r.tick_params(axis='y', labelcolor='red')
    
    ln1, lb1 = ax1.get_legend_handles_labels()
    ln2, lb2 = ax1_r.get_legend_handles_labels()
    ax1.legend(ln1+ln2, lb1+lb2, loc='lower right', framealpha=1.0)

    # Plot 2: DC strategies
    ax2.plot(hours, demand_as_is_fac_mw, linewidth=1.5, label='a) Jobs as-is (with battery)')
    ax2.plot(hours, demand_curtail_fac_mw, linewidth=1.5, label='b) Curtailment-only (with battery)')
    ax2.plot(hours, demand_carbon_fac_mw, linewidth=1.5, label='c) Carbon-aware (with battery)')
    ax2.fill_between(hours, 0, curtailed_supply, alpha=0.3, color='gray', label='Available curtailed (facility MW)')

    ax2.set_title('Datacenter Scheduling Strategies with Battery (Facility MW)', fontsize=14)
    ax2.set_xlabel('Hour')
    ax2.set_ylabel('Power (MW)')
    ax2.set_ylim(0, datacenter_total_capacity_mw * 1.1)
    ax2.grid(True, alpha=0.3)
    ax2.legend()

    # Plot 3: Battery charge/discharge
    ax3.plot(hours, battery_as_is['charge_mw'], linewidth=1.5, label='Charge', linestyle='-', alpha=0.7, color='green')
    ax3.plot(hours, -battery_as_is['discharge_mw'], linewidth=1.5, label='Discharge', linestyle='-', alpha=0.7, color='red')
    ax3.plot(hours, battery_curtail['charge_mw'], linewidth=1.5, linestyle='--', alpha=0.7, color='green')
    ax3.plot(hours, -battery_curtail['discharge_mw'], linewidth=1.5, linestyle='--', alpha=0.7, color='red')
    ax3.plot(hours, battery_carbon['charge_mw'], linewidth=1.5, linestyle=':', alpha=0.7, color='green')
    ax3.plot(hours, -battery_carbon['discharge_mw'], linewidth=1.5, linestyle=':', alpha=0.7, color='red')

    ax3.set_title('Battery Charge/Discharge Patterns', fontsize=14)
    ax3.set_xlabel('Hour')
    ax3.set_ylabel('Power (MW)')
    ax3.grid(True, alpha=0.3)
    ax3.legend()
    ax3.axhline(y=0, color='black', linestyle='-', alpha=0.3)

    # Add day markers to all plots
    for ax in [ax1, ax2, ax3]:
        for d in range(8):
            ax.axvline(d*24, color='gray', linestyle=':', alpha=0.35)

    plt.tight_layout()
    plt.savefig(f'power_usage_comparison_battery_{grid_case}.pdf', dpi=150, bbox_inches='tight')
    print(f"Saved: power_usage_comparison_battery_{grid_case}.pdf")

if __name__ == "__main__":
    main()