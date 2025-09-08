# demo_strategies_combined_plot.py
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
    vmtable_csv = "/z/azure/vmtable.csv"
    grid_case = 'high_curtailment'
    curtail_csv = f"vector_{grid_case}_week_v2.csv"

    datacenter_total_capacity_mw = 20.0
    pue = 1.2
    datacenter_it_capacity_mw = datacenter_total_capacity_mw / pue
    
    # Battery configuration
    battery_capacity_mw = 40.0
    battery_duration_hours = 4.0

    # -----------------------------
    # Load vectors (curtail & carbon)
    # -----------------------------
    print("Loading curtailed power and carbon intensity week vector...")
    curtail_df = pd.read_csv(curtail_csv)
    
    curtailed_supply = curtail_df["Total_Curtailment_NP15_MW"].to_numpy()[:H].astype(float)
    carbon_lbs = curtail_df["marginal_co2_lbs_per_mwh"].to_numpy()
    carbon_intensity_week = (carbon_lbs[:H] * 0.453592).astype(float)

    # Load price data
    if "LMP_NP15" in curtail_df.columns:
        prices = curtail_df["LMP_NP15"].to_numpy()[:H]
    else:
        prices = np.random.uniform(30, 150, H)

    # -----------------------------
    # Build DataCenter
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

    # -----------------------------
    # Scenarios without battery
    # -----------------------------
    def generate_no_battery():
        scenarios = {}
        scenarios['as_is'], _, _ = dc.demand_facility_mw(strategy="as_is", use_battery=False)
        scenarios['curtail'], _, _ = dc.demand_facility_mw(
            strategy="only_curtail", use_battery=False, curtailed_supply_mw=curtailed_supply)
        scenarios['carbon'], _, _ = dc.demand_facility_mw(
            strategy="carbon_aware", use_battery=False, carbon_vector_kg_per_mwh=carbon_intensity_week)
        return scenarios

    # -----------------------------
    # Scenarios with battery
    # -----------------------------
    def generate_with_battery():
        # Add battery
        battery = Battery(
            capacity_mwh=battery_capacity_mw * battery_duration_hours,
            max_charge_mw=battery_capacity_mw,
            max_discharge_mw=battery_capacity_mw,
            round_trip_efficiency=0.9
        )
        dc.battery = battery
        
        scenarios = {}
        scenarios['as_is'], _, scenarios['as_is_batt'] = dc.demand_facility_mw(strategy="as_is", use_battery=True)
        scenarios['curtail'], _, scenarios['curtail_batt'] = dc.demand_facility_mw(
            strategy="only_curtail", use_battery=True, curtailed_supply_mw=curtailed_supply)
        scenarios['carbon'], _, scenarios['carbon_batt'] = dc.demand_facility_mw(
            strategy="carbon_aware", use_battery=True, carbon_vector_kg_per_mwh=carbon_intensity_week)
        return scenarios

    print("Generating scenarios...")
    no_battery = generate_no_battery()
    with_battery = generate_with_battery()

    # -----------------------------
    # Plotting (2x2 subplots)
    # -----------------------------
    print("Creating combined plots...")
    hours = np.arange(H)

    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(8, 6))

    # Plot 1: Price and Carbon Intensity (top-left)
    ax1.plot(hours, prices, linewidth=1.5, label='Electricity Price', color='blue')
    ax1.set_title('Electricity Price and Carbon Intensity', fontsize=12)
    ax1.set_xlabel('Hour')
    ax1.set_ylabel('Price ($/MWh)', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(True, alpha=0.3)

    ax1_r = ax1.twinx()
    ax1_r.plot(hours, carbon_intensity_week, linestyle='--', alpha=0.9, label='Carbon intensity', color='red', zorder=1)
    ax1_r.set_ylabel('Carbon Intensity (kg CO₂/MWh)', color='red')
    ax1_r.tick_params(axis='y', labelcolor='red')
    
    ln1, lb1 = ax1.get_legend_handles_labels()
    ln2, lb2 = ax1_r.get_legend_handles_labels()
    ax1.legend(ln1+ln2, lb1+lb2, loc='lower right', framealpha=1.0, fancybox=False, edgecolor='black', zorder=10)

    # Plot 2: Battery Charge/Discharge (Curtail-only) (top-right)
    ax2.plot(hours, with_battery['curtail_batt']['charge_mw'], linewidth=1.5, label='Charge', color='green')
    ax2.plot(hours, -with_battery['curtail_batt']['discharge_mw'], linewidth=1.5, label='Discharge', color='red')
    ax2.set_title('Battery Charge/Discharge (Curtail-only)', fontsize=12)
    ax2.set_xlabel('Hour')
    ax2.set_ylabel('Power (MW)')
    ax2.grid(True, alpha=0.3)
    ax2.legend()
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)

    # Plot 3: DC strategies without battery (bottom-left)
    ax3.plot(hours, no_battery['as_is'], linewidth=1.5, label='a) Jobs as-is')
    ax3.plot(hours, no_battery['curtail'], linewidth=1.5, label='b) Curtailment-only')
    ax3.plot(hours, no_battery['carbon'], linewidth=1.5, label='c) Carbon-aware')
    ax3.fill_between(hours, 0, curtailed_supply, alpha=0.3, color='gray', label='Curtailment available')

    ax3.set_title('Datacenter Scheduling Strategies', fontsize=12)
    ax3.set_xlabel('Hour')
    ax3.set_ylabel('Power (MW)')
    ax3.set_ylim(0, datacenter_total_capacity_mw * 1.1)
    ax3.grid(True, alpha=0.3)
    ax3.legend()

    # Plot 4: DC strategies with battery (bottom-right)
    ax4.plot(hours, with_battery['as_is'], linewidth=1.5, label='a) Jobs as-is (with battery)')
    ax4.plot(hours, with_battery['curtail'], linewidth=1.5, label='b) Curtailment-only (with battery)')
    ax4.plot(hours, with_battery['carbon'], linewidth=1.5, label='c) Carbon-aware (with battery)')
    ax4.fill_between(hours, 0, curtailed_supply, alpha=0.3, color='gray', label='Curtailment available')

    ax4.set_title('Datacenter Scheduling Strategies with Battery', fontsize=12)
    ax4.set_xlabel('Hour')
    ax4.set_ylabel('Power (MW)')
    ax4.set_ylim(0, datacenter_total_capacity_mw * 1.1)
    ax4.grid(True, alpha=0.3)
    ax4.legend()

    # Add day markers to all plots
    for ax in [ax1, ax2, ax3, ax4]:
        for d in range(8):
            ax.axvline(d*24, color='gray', linestyle=':', alpha=0.35)

    plt.tight_layout()
    plt.savefig(f'power_usage_comparison_combined_{grid_case}.pdf', dpi=150, bbox_inches='tight')
    print(f"Saved: power_usage_comparison_combined_{grid_case}.pdf")

if __name__ == "__main__":
    main()