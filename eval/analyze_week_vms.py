import pandas as pd
import numpy as np
from multiprocessing import Pool, cpu_count
from datetime import datetime
import time

# Power model parameter
WATTS_PER_VCPU = 20.0  # W per vCPU at 100% utilization

# Define time window (week 2: day 7-14)
WEEK_START = 7 * 24 * 3600  # 1 week later
WEEK_END = 14 * 24 * 3600  # 2 weeks later

def process_chunk(chunk_info):
    """Process a single chunk"""
    chunk_start, chunk_size = chunk_info
    
    # Read chunk
    df = pd.read_csv("./earliest_vm_readings_merged.csv", 
                     skiprows=range(1, chunk_start), 
                     nrows=chunk_size)
    
    if len(df) == 0:
        return None
    
    # Clean column names
    df.columns = [c.strip().lower() for c in df.columns]
    
    # Filter required columns and convert data types
    required_cols = ['vm_id', 'timestamp vm created', 'timestamp vm deleted', 'avg_cpu', 'vm virtual core count bucket']
    if not all(col in df.columns for col in required_cols):
        return None
    
    df = df[required_cols].copy()
    
    # Convert to numeric
    for col in ['timestamp vm created', 'timestamp vm deleted', 'avg_cpu', 'vm virtual core count bucket']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Remove invalid data
    df = df.dropna()
    df = df[df['avg_cpu'].between(0, 100)]
    df = df[df['vm virtual core count bucket'] > 0]
    
    # Check timestamp range to determine units (print once per chunk for debugging)
    if len(df) > 0:
        min_ts = df['timestamp vm created'].min()
        max_ts = df['timestamp vm created'].max()
        # If values are small (< 1e10), likely seconds since epoch
        # If large (> 1e15), likely microseconds since epoch
    
    # Filter VMs running within target week
    week_vms = df[
        (df['timestamp vm created'] < WEEK_END) & 
        (df['timestamp vm deleted'] > WEEK_START)
    ].copy()
    
    if len(week_vms) == 0:
        return None
    
    # Calculate actual runtime for each VM within the week
    week_vms['actual_start'] = np.maximum(week_vms['timestamp vm created'], WEEK_START)
    week_vms['actual_end'] = np.minimum(week_vms['timestamp vm deleted'], WEEK_END)
    week_vms['runtime_hours'] = (week_vms['actual_end'] - week_vms['actual_start']) / 3600
    
    # Calculate power consumption using parameter
    week_vms['power_w'] = week_vms['vm virtual core count bucket'] * week_vms['avg_cpu'] / 100 * WATTS_PER_VCPU
    week_vms['energy_wh'] = week_vms['power_w'] * week_vms['runtime_hours']
    
    return week_vms[['vm_id', 'actual_start', 'actual_end', 'runtime_hours', 
                     'avg_cpu', 'vm virtual core count bucket', 'power_w', 'energy_wh']]

def parallel_process_csv():
    """Process CSV file in parallel"""
    print("Starting parallel CSV processing...")
    
    # Get total file line count
    print("Counting file lines...")
    with open("./earliest_vm_readings_merged.csv", 'r') as f:
        total_lines = sum(1 for _ in f) - 1  # Subtract header
    
    print(f"Total lines: {total_lines:,}")
    
    # Set chunk size and process count
    chunk_size = 100000
    n_processes = min(cpu_count(), 8)  # Maximum 8 processes
    
    # Create chunk information
    chunks = []
    for i in range(1, total_lines + 1, chunk_size):  # Start from 1 to skip header
        chunks.append((i, min(chunk_size, total_lines - i + 1)))
    
    print(f"Split into {len(chunks)} chunks, using {n_processes} processes")
    
    # Parallel processing
    start_time = time.time()
    with Pool(n_processes) as pool:
        results = pool.map(process_chunk, chunks)
    
    # Merge results
    valid_results = [r for r in results if r is not None]
    if not valid_results:
        print("No VMs found matching criteria")
        return None
    
    week_vms = pd.concat(valid_results, ignore_index=True)
    processing_time = time.time() - start_time
    
    print(f"Processing completed, time taken: {processing_time:.1f} seconds")
    print(f"Found {len(week_vms):,} VMs running within target week")
    
    return week_vms

def analyze_vms(week_vms):
    """Analyze VM data"""
    print("\n=== VM Analysis Results ===")
    
    # Basic statistics
    print(f"Total VMs: {len(week_vms):,}")
    print(f"Total runtime: {week_vms['runtime_hours'].sum():.1f} hours")
    print(f"Average runtime: {week_vms['runtime_hours'].mean():.1f} hours")
    
    # CPU usage
    print(f"\nCPU utilization:")
    print(f"  Average: {week_vms['avg_cpu'].mean():.1f}%")
    print(f"  Maximum: {week_vms['avg_cpu'].max():.1f}%")
    print(f"  Minimum: {week_vms['avg_cpu'].min():.1f}%")
    
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
        start_hour = int((vm['actual_start'] - WEEK_START) // 3600)
        end_hour = int(np.ceil((vm['actual_end'] - WEEK_START) / 3600))
        
        start_hour = max(0, start_hour)
        end_hour = min(168, end_hour)
        
        for h in range(start_hour, end_hour):
            hourly_power[h] += vm['power_w']
    
    return hourly_power

if __name__ == "__main__":
    # Process CSV in parallel
    week_vms = parallel_process_csv()
    
    if week_vms is not None:
        # Analyze results
        week_vms, hourly_power = analyze_vms(week_vms)
        
        # Save results
        week_vms.to_csv("week_vms_analysis.csv", index=False)
        np.save("hourly_power.npy", hourly_power)
        
        print(f"\nResults saved to:")
        print(f"  week_vms_analysis.csv - VM detailed data")
        print(f"  hourly_power.npy - Hourly power data")
    else:
        print("No data found matching criteria")