import numpy as np
import pandas as pd
from datacenter import DataCenter, DataCenterConfig
from battery import Battery
from datetime import datetime

def load_price_and_curtailment_data():
    """Load LMP and curtailment data"""
    # Load price data
    price_df = pd.read_csv("vector_high_volatility_week.csv")
    price_vector = price_df['lmp_$/mwh'].values[:168]  # First week
    
    # Load curtailment data  
    curtail_df = pd.read_csv("vector_high_curtailment_week.csv")
    curtailed_supply = curtail_df['curtailed_mw'].values[:168]  # First week
    
    return price_vector, curtailed_supply



def analyze_strategy(strategy_name, dc, battery_capacity_mw, price_vector, curtailed_supply, carbon_vector, bess_params):
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
    
    # Simulate with the demand and battery using price vector for arbitrage
    result = dc.simulate(
        curtailed_supply_mw=curtailed_supply,
        price_vector_per_mwh=price_vector,  # Pass price vector for arbitrage
        grid_ci_kg_per_mwh=400.0,  # Grid carbon intensity
        curtailed_ci_kg_per_mwh=0.0  # Clean curtailed energy
    )
    # Override with our calculated demand
    result['demand_mw'] = demand_mw
    
    # Calculate battery costs
    battery_capex = 0
    if battery_capacity_mw > 0:
        battery_energy_kwh = battery_capacity_mw * 1000 * BESS_DURATION_HOURS
        battery_power_kw = battery_capacity_mw * 1000
        battery_capex = (battery_energy_kwh * BESS_COST_PER_KWH + 
                        battery_power_kw * BESS_FIRE_SUPPRESSION_COST_PER_KW)
    
    # Calculate metrics
    total_jobs_scheduled = np.sum(result['demand_mw'] > 0)  # Simplified job count
    total_cost = result.attrs['totals']['total_cost_usd'] + battery_capex
    total_carbon = result.attrs['totals']['total_emissions_kg']
    
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
        'total_energy_mwh': result.attrs['totals']['total_energy_mwh']
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