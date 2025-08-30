import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from datacenter import DataCenter, DataCenterConfig
from datetime import datetime

# Load the analyzed hourly power data
print("Loading analyzed VM data...")
hourly_power_w = np.load("hourly_power.npy")
hourly_power_mw = hourly_power_w / 1e6

datacenter_total_capacity_mw = 20.0
pue = 1.2
datacenter_it_capacity_mw = datacenter_total_capacity_mw / pue

# Create datacenter config matching the analysis
config = DataCenterConfig(
    capacity_mw=datacenter_it_capacity_mw,
    pue=pue, 
    week_start=datetime(1970, 1, 1),
    timezone='UTC'
)

# Create datacenter using the original VM data
dc = DataCenter(csv_path="/z/azure/vmtable.csv", config=config, scale_jobs=True)

# Load real curtailment data
H = config.week_hours
curtail_df = pd.read_csv("vector_high_curtailment_week.csv")
curtailed_supply = curtail_df['curtailed_mw'].values[:H]  # Take first 168 hours

print("Generating scheduling scenarios...")

# Scenario 1: Original trace (from analysis)
raw_demand_mw = hourly_power_mw

# Scenario 2a: Jobs scaled to 20MW capacity (run as-is)
scaled_demand_mw = dc.demand_facility_mw()

# Scenario 2b: Curtailment-only scheduling using datacenter's scheduling method
curtail_demand_mw = dc.demand_facility_mw(only_curtail=True, curtailed_supply_mw=curtailed_supply) 

print("Creating comparison plots...")
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 10))

# Plot 1: Original job trace power usage
hours = np.arange(H)
ax1.plot(hours, raw_demand_mw, 'b-', linewidth=1.5, label='Original VM trace')
ax1.set_title('Plot 1: Original Job Trace Power Usage (Unmodified)', fontsize=14)
ax1.set_xlabel('Hour')
ax1.set_ylabel('Power (MW)')
ax1.grid(True, alpha=0.3)
ax1.legend()

# Add day markers
for day in range(8):
    ax1.axvline(x=day*24, color='gray', linestyle=':', alpha=0.5)
    if day < 7:
        ax1.text(day*24 + 12, ax1.get_ylim()[1]*0.9, f'Day {day+1}', 
                ha='center', fontsize=10, alpha=0.7)

# Plot 2: Datacenter scheduling strategies
ax2.plot(hours, scaled_demand_mw, 'g-', linewidth=1.5, label='a) Jobs as-is')
ax2.plot(hours, curtail_demand_mw, 'r-', linewidth=1.5, label='b) Curtailment-only scheduling')
ax2.plot(hours, curtailed_supply, 'orange', linestyle='--', alpha=0.7, linewidth=1, 
         label='Available curtailed power')

ax2.set_title('Plot 2: Datacenter Scheduling Strategies', fontsize=14)
ax2.set_xlabel('Hour')
ax2.set_ylabel('Power (MW)')
ax2.set_ylim(0, 21)
ax2.legend()
ax2.grid(True, alpha=0.3)

# Add day markers
for day in range(8):
    ax2.axvline(x=day*24, color='gray', linestyle=':', alpha=0.5)

plt.tight_layout()
plt.savefig('power_usage_comparison.png', dpi=150, bbox_inches='tight')
print("Comparison charts saved to 'power_usage_comparison.png'.")

# Print summary statistics
print(f"\n=== Summary Statistics ===")
print(f"Original trace:")
print(f"  Peak power: {raw_demand_mw.max():.2f} MW")
print(f"  Average power: {raw_demand_mw.mean():.2f} MW")
print(f"  Total energy: {raw_demand_mw.sum():.1f} MWh")

print(f"\nScaled to datacenter capacity:")
print(f"  Peak power: {scaled_demand_mw.max():.2f} MW")
print(f"  Average power: {scaled_demand_mw.mean():.2f} MW") 
print(f"  Total energy: {scaled_demand_mw.sum():.1f} MWh")

print(f"\nCurtailment-only scheduling:")
print(f"  Peak power: {curtail_demand_mw.max():.2f} MW")
print(f"  Average power: {curtail_demand_mw.mean():.2f} MW")
print(f"  Total energy: {curtail_demand_mw.sum():.1f} MWh")
print(f"  Curtailed energy used: {curtail_demand_mw.sum():.1f} MWh")
print(f"  Energy utilization: {curtail_demand_mw.sum()/curtailed_supply.sum()*100:.1f}%")