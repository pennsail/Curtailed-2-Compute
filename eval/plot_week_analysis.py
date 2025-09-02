import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

# Load analysis results
print("Loading analysis results...")
week_vms = pd.read_csv("azure_vms_analysis.csv")
hourly_power = np.load("azure_hourly_power.npy")

# Convert to MW
hourly_power_mw = hourly_power / 1e6

print(f"Data overview:")
print(f"  Total VMs: {len(week_vms):,}")
print(f"  Average hourly power: {hourly_power_mw.mean():.2f} MW")
print(f"  Peak power: {hourly_power_mw.max():.2f} MW")

# Create charts
fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))

# 1. Hourly power variation
hours = np.arange(168)
ax1.plot(hours, hourly_power_mw, 'b-', linewidth=1)
ax1.set_title('Hourly Power Consumption Over One Week')
ax1.set_xlabel('Hour (0-167)')
ax1.set_ylabel('Power (MW)')
ax1.grid(True, alpha=0.3)

# Add day markers
for day in range(8):
    ax1.axvline(x=day*24, color='red', linestyle='--', alpha=0.5)
    if day < 7:
        ax1.text(day*24 + 12, ax1.get_ylim()[1]*0.9, f'Day {day+1}', 
                ha='center', fontsize=8)

# 2. Runtime distribution by vCPU groups
top_vcpu_groups = week_vms['vm virtual core count bucket'].value_counts().head(4).index
colors = ['red', 'blue', 'green', 'orange']
for i, vcpu in enumerate(top_vcpu_groups):
    data = week_vms[week_vms['vm virtual core count bucket'] == vcpu]['runtime_hours']
    ax2.hist(data, bins=30, alpha=0.6, label=f'{int(vcpu)} vCPUs', color=colors[i])
ax2.set_title('Runtime Distribution by vCPU Count')
ax2.set_xlabel('Runtime (hours)')
ax2.set_ylabel('Number of VMs (log scale)')
ax2.set_yscale('log')
ax2.legend()
ax2.grid(True, alpha=0.3)

# 3. vCPU distribution
vcpu_counts = week_vms['vm virtual core count bucket'].value_counts().sort_index()
top_vcpus = vcpu_counts.head(10)
ax3.bar(range(len(top_vcpus)), top_vcpus.values, color='orange')
ax3.set_title('vCPU Configuration Distribution (Top 10)')
ax3.set_xlabel('Number of vCPUs')
ax3.set_ylabel('Number of VMs (log scale)')
ax3.set_yscale('log')
ax3.set_xticks(range(len(top_vcpus)))
ax3.set_xticklabels([f'{int(x)}' for x in top_vcpus.index])
ax3.grid(True, alpha=0.3)

# 4. vCPU hours histogram
week_vms['vcpu_hours'] = week_vms['vm virtual core count bucket'] * week_vms['runtime_hours']
ax4.hist(week_vms['vcpu_hours'], bins=50, alpha=0.7, color='purple', edgecolor='black', linewidth=0.5)
ax4.set_title('vCPU Hours Distribution')
ax4.set_xlabel('vCPU Hours')
ax4.set_ylabel('Number of VMs (log scale)')
ax4.set_yscale('log')
ax4.grid(True, alpha=0.3)
# Add statistics text
ax4.text(0.7, 0.8, f'Mean: {week_vms["vcpu_hours"].mean():.1f}\nMedian: {week_vms["vcpu_hours"].median():.1f}', 
         transform=ax4.transAxes, bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

plt.tight_layout()
plt.savefig('week_vm_analysis.png', dpi=150, bbox_inches='tight')
print("Charts saved to 'week_vm_analysis.png'.")

# Print statistical summary
print(f"\n=== Statistical Summary ===")
print(f"Total energy: {hourly_power_mw.sum():.1f} MWh")
print(f"Average power: {hourly_power_mw.mean():.2f} MW")
print(f"Peak power: {hourly_power_mw.max():.2f} MW")
print(f"Minimum power: {hourly_power_mw.min():.2f} MW")
print(f"Power variation range: {hourly_power_mw.max() - hourly_power_mw.min():.2f} MW")

# VM runtime statistics
print(f"\nVM Runtime Statistics:")
print(f"  Mean runtime: {week_vms['runtime_hours'].mean():.1f} hours")
print(f"  Median runtime: {week_vms['runtime_hours'].median():.1f} hours")
print(f"  Max runtime: {week_vms['runtime_hours'].max():.1f} hours")
print(f"  VMs running < 1h: {(week_vms['runtime_hours'] < 1).sum():,} ({(week_vms['runtime_hours'] < 1).mean()*100:.1f}%)")
print(f"  VMs running full week: {(week_vms['runtime_hours'] >= 168).sum():,} ({(week_vms['runtime_hours'] >= 168).mean()*100:.1f}%)")