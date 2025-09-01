import numpy as np
import pandas as pd
from datacenter import DataCenter, DataCenterConfig
from battery import Battery
from datetime import datetime

def load_price_and_curtailment_data():
    """Load LMP and curtailment data"""
    # Load curtailment data (which also has price)
    curtail_df = pd.read_csv("vector_high_curtailment_week.csv")
    price_vector = curtail_df['LMP_NP15'].values[:168]  # First week
    curtailed_supply = curtail_df['Total_Curtailment_NP15_MW'].values[:168]  # First week
    
    return price_vector, curtailed_supply



def analyze_strategy(strategy_name, dc: DataCenter, battery_capacity_mw, price_vector, curtailed_supply, carbon_vector, bess_params):
    """Analyze a single strategy with given battery capacity"""
    
    BESS_DURATION_HOURS, BESS_COST_PER_KWH, BESS_FIRE_SUPPRESSION_COST_PER_KW = bess_params
    
    # Create battery if capacity > 0
    battery = Battery(capacity_mwh=battery_capacity_mw * BESS_DURATION_HOURS, max_charge_mw=battery_capacity_mw, 
                     max_discharge_mw=battery_capacity_mw) if battery_capacity_mw > 0 else None
    dc.battery = battery
    use_battery = battery_capacity_mw > 0
    
    # Get demand using new API
    if strategy_name == "as_is":
        demand_mw = dc.demand_facility_mw(
            only_curtail=False,
            use_battery=False,  # as_is doesn't use battery in scheduling
            curtailed_supply_mw=curtailed_supply,
            price_vector_per_mwh=price_vector
        )
    else:  # curtail_only
        demand_mw = dc.demand_facility_mw(
            only_curtail=True,
            use_battery=use_battery,
            curtailed_supply_mw=curtailed_supply,
            price_vector_per_mwh=price_vector
        )
    
    # Calculate metrics directly from demand
    total_energy_mwh = float(np.sum(demand_mw))
    avg_price = float(np.mean(price_vector)) if len(price_vector) > 0 else 80.0
    total_cost = total_energy_mwh * avg_price
    total_carbon = total_energy_mwh * 400.0  # kg CO2/MWh
    
    # Create result structure
    result = {
        'demand_mw': demand_mw,
        'attrs': {
            'totals': {
                'total_energy_mwh': total_energy_mwh,
                'total_cost_usd': total_cost,
                'total_emissions_kg': total_carbon
            }
        }
    }
    
    # Calculate battery costs
    battery_capex = 0
    if battery_capacity_mw > 0:
        battery_energy_kwh = battery_capacity_mw * 1000 * BESS_DURATION_HOURS
        battery_power_kw = battery_capacity_mw * 1000
        battery_capex = (battery_energy_kwh * BESS_COST_PER_KWH + 
                        battery_power_kw * BESS_FIRE_SUPPRESSION_COST_PER_KW)
    
    # Calculate metrics - get actual job count
    if dc._jobs is None:
        dc._extract_jobs_from_vms()
    all_jobs = dc._get_scaled_jobs() if dc.scale_jobs else dc._jobs
    
    if strategy_name == "as_is":
        total_jobs_scheduled = len(all_jobs)
    else:
        # For curtail_only, estimate from energy ratio
        as_is_energy = len(all_jobs) * np.mean([j.it_power_mw * j.duration_h for j in all_jobs])
        curtail_energy = total_energy_mwh / dc.config.pue  # Convert back to IT energy
        total_jobs_scheduled = max(1, int(len(all_jobs) * curtail_energy / as_is_energy)) if as_is_energy > 0 else 1
    
    total_cost = result['attrs']['totals']['total_cost_usd'] + battery_capex
    total_carbon = result['attrs']['totals']['total_emissions_kg']
    
    cost_per_job = total_cost / max(total_jobs_scheduled, 1)
    carbon_per_job = total_carbon / max(total_jobs_scheduled, 1)
    
    return {
        'strategy': strategy_name,
        'battery_capacity_mw': battery_capacity_mw,
        'battery_capex_usd': battery_capex,
        'total_jobs_scheduled': total_jobs_scheduled,
        'total_cost_usd': total_cost,
        'total_carbon_kg': total_carbon,
        'cost_per_job': cost_per_job,
        'carbon_per_job': carbon_per_job,
        'total_energy_mwh': result['attrs']['totals']['total_energy_mwh']
    }

def main():
    """Run battery cost-benefit analysis"""
    
    # Load data
    price_vector, curtailed_supply = load_price_and_curtailment_data()
    carbon_vector = np.full(168, 400.0)  # kg CO2/MWh
    
    # BESS Economic Parameters
    BESS_DURATION_HOURS = 4
    BESS_COST_PER_KWH = 150  # $/kWh
    BESS_FIRE_SUPPRESSION_COST_PER_KW = 8  # $/kW
    
    # Setup datacenter
    config = DataCenterConfig(capacity_mw=20.0, pue=1.2, week_start=datetime(1970, 1, 1), timezone='UTC')
    dc = DataCenter(csv_path="/z/azure/vmtable.csv", config=config, scale_jobs=True)
    
    # Battery capacities to test
    battery_capacities = [0, 5, 10, 20]  # MW
    strategies = ["as_is", "curtail_only"]
    
    results = []
    bess_params = (BESS_DURATION_HOURS, BESS_COST_PER_KWH, BESS_FIRE_SUPPRESSION_COST_PER_KW)
    
    for strategy in strategies:
        print(f"\n=== Analyzing {strategy} strategy ===")
        
        for battery_mw in battery_capacities:
            print(f"Testing battery capacity: {battery_mw} MW")
            
            try:
                result = analyze_strategy(strategy, dc, battery_mw, price_vector, 
                                        curtailed_supply, carbon_vector, bess_params)
                results.append(result)
                
                print(f"  Jobs scheduled: {result['total_jobs_scheduled']}")
                print(f"  Battery CapEx: ${result['battery_capex_usd']:.2f}")
                print(f"  Total cost: ${result['total_cost_usd']:.2f}")
                print(f"  Cost per job: ${result['cost_per_job']:.2f}")
                print(f"  Carbon per job: {result['carbon_per_job']:.2f} kg")
                
            except Exception as e:
                import traceback
                print(f"  Error: {e}")
                print(f"  Traceback: {traceback.format_exc()}")
    
    # Save results
    results_df = pd.DataFrame(results)
    results_df.to_csv("battery_analysis_results.csv", index=False)
    
    print(f"\n=== Summary ===")
    print(results_df.to_string(index=False))
    print(f"\nResults saved to battery_analysis_results.csv")

if __name__ == "__main__":
    main()