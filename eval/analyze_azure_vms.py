# analyze_azure_vms.py
# Pre-processing script for the raw Azure VM Traces public dataset.
# Download vmtable.csv from: https://github.com/Azure/AzurePublicDataset
# Then set VMTABLE_PATH below (or place vmtable.csv in this directory).
import pandas as pd
import numpy as np
from datetime import datetime
import time

VMTABLE_PATH = "vmtable.csv"  # update to the full Azure dataset path for full-scale analysis

# Power model parameter
WATTS_PER_VCPU = 20.0  # W per vCPU at 100% utilization

# Define first week (7 days = 604800 seconds)
WEEK_START = 0
WEEK_END = 7 * 24 * 3600  # 604800 seconds

def load_azure_vms():
    """Load and process Azure VM data"""
    print("Loading Azure VM data...")
    
    column_headers = [
        'vm id', 'subscription id', 'deployment id', 'timestamp vm created', 'timestamp vm deleted',
        'max cpu', 'avg cpu', 'p95 max cpu', 'vm category', 'vm virtual core count bucket', 'vm memory (gb) bucket'
    ]
    vmtable_df = pd.read_csv(VMTABLE_PATH, header=None)
    vmtable_df.columns = column_headers
    
    print(f"Loaded {len(vmtable_df):,} VMs")
    
    # Convert to numeric and clean data
    vmtable_df['timestamp vm created'] = pd.to_numeric(vmtable_df['timestamp vm created'], errors='coerce')
    vmtable_df['timestamp vm deleted'] = pd.to_numeric(vmtable_df['timestamp vm deleted'], errors='coerce')
    vmtable_df['avg cpu'] = pd.to_numeric(vmtable_df['avg cpu'], errors='coerce')
    vmtable_df['vm virtual core count bucket'] = pd.to_numeric(vmtable_df['vm virtual core count bucket'], errors='coerce')
    
    # Remove invalid data
    vmtable_df = vmtable_df.dropna(subset=['timestamp vm created', 'timestamp vm deleted', 'avg cpu', 'vm virtual core count bucket'])
    vmtable_df = vmtable_df[vmtable_df['avg cpu'].between(0, 100)]
    vmtable_df = vmtable_df[vmtable_df['vm virtual core count bucket'] > 0]
    
    print(f"After cleaning: {len(vmtable_df):,} VMs")
    
    # Calculate job duration
    vmtable_df['duration'] = vmtable_df['timestamp vm deleted'] - vmtable_df['timestamp vm created']
    
    # Shift all jobs to start within first week
    # Find earliest start time
    min_start = vmtable_df['timestamp vm created'].min()
    
    # Calculate shift amount to map jobs to first week
    # Use modulo to wrap jobs that start later into the first week
    vmtable_df['week_offset'] = ((vmtable_df['timestamp vm created'] - min_start) // WEEK_END)
    vmtable_df['shifted_start'] = (vmtable_df['timestamp vm created'] - min_start) % WEEK_END
    vmtable_df['shifted_end'] = vmtable_df['shifted_start'] + vmtable_df['duration']
    
    # Clip jobs that extend beyond first week
    vmtable_df['actual_start'] = vmtable_df['shifted_start']
    vmtable_df['actual_end'] = np.minimum(vmtable_df['shifted_end'], WEEK_END)
    vmtable_df['runtime_hours'] = (vmtable_df['actual_end'] - vmtable_df['actual_start']) / 3600
    
    # Filter out jobs with no runtime in first week
    vmtable_df = vmtable_df[vmtable_df['runtime_hours'] > 0]
    
    print(f"After shifting to first week: {len(vmtable_df):,} VMs")
    
    return vmtable_df

def analyze_vms(week_vms):
    """Analyze VM data"""
    print("\n=== VM Analysis Results ===")
    
    # Calculate power consumption
    week_vms['power_w'] = week_vms['vm virtual core count bucket'] * week_vms['avg cpu'] / 100 * WATTS_PER_VCPU
    week_vms['energy_wh'] = week_vms['power_w'] * week_vms['runtime_hours']
    
    # Basic statistics
    print(f"Total VMs: {len(week_vms):,}")
    print(f"Total runtime: {week_vms['runtime_hours'].sum():.1f} hours")
    print(f"Average runtime: {week_vms['runtime_hours'].mean():.1f} hours")
    
    # CPU usage
    print(f"\nCPU utilization:")
    print(f"  Average: {week_vms['avg cpu'].mean():.1f}%")
    print(f"  Maximum: {week_vms['avg cpu'].max():.1f}%")
    print(f"  Minimum: {week_vms['avg cpu'].min():.1f}%")
    
    # vCPU distribution
    print(f"\nvCPU distribution:")
    vcpu_dist = week_vms['vm virtual core count bucket'].value_counts().sort_index()
    for vcpu, count in vcpu_dist.head(10).items():
        print(f"  {vcpu} vCPU: {count:,} VMs")
    
    # Power consumption
    total_energy_kwh = week_vms['energy_wh'].sum() / 1000
    avg_power_w = week_vms['power_w'].mean()
    max_power_w = week_vms['power_w'].max()
    
    print(f"\nPower consumption:")
    print(f"  Total energy: {total_energy_kwh:.1f} kWh")
    print(f"  Average power: {avg_power_w:.1f} W")
    print(f"  Maximum power: {max_power_w:.1f} W")
    
    # Calculate hourly power distribution
    print("\nCalculating hourly power distribution...")
    hourly_power = calculate_hourly_power(week_vms)
    
    print(f"Hourly power statistics:")
    print(f"  Average: {hourly_power.mean():.1f} W")
    print(f"  Maximum: {hourly_power.max():.1f} W")
    print(f"  Minimum: {hourly_power.min():.1f} W")
    
    return week_vms, hourly_power

def calculate_hourly_power(week_vms):
    """Calculate total power for each hour"""
    hourly_power = np.zeros(7 * 24)  # 168 hours
    
    for _, vm in week_vms.iterrows():
        start_hour = int(vm['actual_start'] // 3600)
        end_hour = int(np.ceil(vm['actual_end'] / 3600))
        
        start_hour = max(0, start_hour)
        end_hour = min(168, end_hour)
        
        for h in range(start_hour, end_hour):
            hourly_power[h] += vm['power_w']
    
    return hourly_power

if __name__ == "__main__":
    # Load Azure VM data
    week_vms = load_azure_vms()
    
    if week_vms is not None and len(week_vms) > 0:
        # Analyze results
        week_vms, hourly_power = analyze_vms(week_vms)
        
        # Save results
        week_vms.to_csv("azure_vms_analysis.csv", index=False)
        np.save("azure_hourly_power.npy", hourly_power)
        
        print(f"\nResults saved to:")
        print(f"  azure_vms_analysis.csv - VM detailed data")
        print(f"  azure_hourly_power.npy - Hourly power data")
    else:
        print("No data found matching criteria")