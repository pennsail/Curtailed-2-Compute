import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def calculate_total_cost_with_capex():
    """Calculate total cost including amortized CapEx for datacenter and battery"""
    
    # Load results
    df = pd.read_csv("battery_analysis_results_high_curtailment.csv")
    
    # Datacenter CapEx (amortized over 10 years to weekly basis)
    DC_LAND_COST = 2_250_000
    DC_CONSTRUCTION_COST = 180_000_000
    DC_SITE_PREP_COST = 600_000
    DC_FIBER_COST = 60_000
    
    total_dc_capex = DC_LAND_COST + DC_CONSTRUCTION_COST + DC_SITE_PREP_COST + DC_FIBER_COST
    weekly_dc_capex = total_dc_capex / (10 * 52)  # 10 years, 52 weeks per year
    
    # Battery CapEx parameters
    BESS_DURATION_HOURS = 4
    BESS_COST_PER_KWH = 150
    BESS_FIRE_SUPPRESSION_COST_PER_KW = 8
    
    # Calculate total cost with CapEx
    results = []
    for _, row in df.iterrows():
        battery_mw = row['battery_capacity_mw']
        
        # Battery CapEx (amortized over 10 years to weekly basis)
        if battery_mw > 0:
            battery_energy_kwh = battery_mw * 1000 * BESS_DURATION_HOURS
            battery_power_kw = battery_mw * 1000
            battery_capex = (battery_energy_kwh * BESS_COST_PER_KWH + 
                           battery_power_kw * BESS_FIRE_SUPPRESSION_COST_PER_KW)
            weekly_battery_capex = battery_capex / (10 * 52)
        else:
            weekly_battery_capex = 0
        
        # Total weekly cost = OpEx + amortized CapEx
        total_weekly_cost = row['total_cost_usd'] + weekly_dc_capex + weekly_battery_capex
        
        results.append({
            'strategy': row['strategy'],
            'battery_capacity_mw': battery_mw,
            'total_jobs_scheduled': row['total_jobs_scheduled'],
            'opex_usd': row['total_cost_usd'],
            'weekly_dc_capex': weekly_dc_capex,
            'weekly_battery_capex': weekly_battery_capex,
            'total_weekly_cost': total_weekly_cost,
            'total_carbon_kg': row['total_carbon_kg'],
            'cost_per_job': total_weekly_cost / max(row['total_jobs_scheduled'], 1),
            'carbon_per_job': row['carbon_per_job'],
            'total_energy_mwh': row['total_energy_mwh']
        })
    
    return pd.DataFrame(results)

def plot_battery_impact():
    """Create 1x2 plot showing battery impact analysis"""
    
    df = calculate_total_cost_with_capex()
    
    # Setup plot - 1 row, 2 columns
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4))
    
    strategies = df['strategy'].unique()
    colors = {'as_is': 'blue', 'only_curtail': 'red', 'carbon_aware': 'green'}
    
    # Left plot: Total Cost (left y-axis) and Carbon (right y-axis)
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        ax1.plot(strategy_data['battery_capacity_mw'], strategy_data['total_weekly_cost']/1000, 
                'o-', color=colors[strategy], label=f'{strategy} (Cost)', linewidth=2, markersize=4)
    
    ax1.set_xlabel('Battery Capacity (MW)')
    ax1.set_ylabel('Total Weekly Cost ($k)', color='black')
    ax1.tick_params(axis='y', labelcolor='black')
    ax1.grid(True, alpha=0.3)
    
    # Create second y-axis for carbon
    ax1_twin = ax1.twinx()
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        ax1_twin.plot(strategy_data['battery_capacity_mw'], strategy_data['total_carbon_kg'],
                     's-', color=colors[strategy], label=f'{strategy} (Carbon)', linewidth=3, markersize=6, alpha=1.0, dashes=[5, 5])
    
    ax1_twin.set_ylabel('Total Carbon (kg CO2)', color='gray')
    ax1_twin.tick_params(axis='y', labelcolor='gray')
    
    # Combine legends
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_twin.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='center right', fontsize=8)
    
    ax1.set_title('Total Cost & Carbon vs Battery Capacity')
    
    # Right plot: Cost per Job (left y-axis) and Carbon per Job (right y-axis)
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        ax2.plot(strategy_data['battery_capacity_mw'], strategy_data['cost_per_job'],
                'o-', color=colors[strategy], label=f'{strategy} (Cost)', linewidth=2, markersize=4)
    
    ax2.set_xlabel('Battery Capacity (MW)')
    ax2.set_ylabel('Cost per Job ($)', color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    ax2.grid(True, alpha=0.3)
    
    # Create second y-axis for carbon per job
    ax2_twin = ax2.twinx()
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        ax2_twin.plot(strategy_data['battery_capacity_mw'], strategy_data['carbon_per_job'],
                     's--', color=colors[strategy], label=f'{strategy} (Carbon)', linewidth=3, markersize=6, alpha=1.0, dashes=[5, 5])
    
    ax2_twin.set_ylabel('Carbon per Job (kg CO2)', color='gray')
    ax2_twin.tick_params(axis='y', labelcolor='gray')
    
    # Combine legends
    lines1, labels1 = ax2.get_legend_handles_labels()
    lines2, labels2 = ax2_twin.get_legend_handles_labels()
    ax2.legend(lines1 + lines2, labels1 + labels2, loc='center right', fontsize=8)
    
    ax2.set_title('Per-Job Cost & Carbon vs Battery Capacity')
    
    plt.tight_layout()
    plt.savefig('battery_impact_analysis.pdf', dpi=150, bbox_inches='tight')
    print("Battery impact analysis saved to 'battery_impact_analysis.pdf'")
    
    return df

def print_summary(df):
    """Print summary statistics"""
    print("\n=== Battery Impact Analysis Summary ===")
    
    # Weekly datacenter CapEx
    total_dc_capex = 2_250_000 + 180_000_000 + 600_000 + 60_000
    weekly_dc_capex = total_dc_capex / (10 * 52)
    print(f"Weekly Datacenter CapEx (amortized over 10 years): ${weekly_dc_capex:,.0f}")
    
    # Battery costs
    print(f"\nBattery CapEx by capacity:")
    for battery_mw in sorted(df['battery_capacity_mw'].unique()):
        if battery_mw > 0:
            battery_data = df[df['battery_capacity_mw'] == battery_mw].iloc[0]
            print(f"  {battery_mw} MW: ${battery_data['weekly_battery_capex']:,.0f}/week")
    
    # Cost breakdown by strategy
    print(f"\nCost breakdown by strategy (20 MW battery):")
    for strategy in df['strategy'].unique():
        strategy_20mw = df[(df['strategy'] == strategy) & (df['battery_capacity_mw'] == 20)]
        if not strategy_20mw.empty:
            row = strategy_20mw.iloc[0]
            print(f"  {strategy}:")
            print(f"    OpEx: ${row['opex_usd']:,.0f}")
            print(f"    DC CapEx: ${row['weekly_dc_capex']:,.0f}")
            print(f"    Battery CapEx: ${row['weekly_battery_capex']:,.0f}")
            print(f"    Total: ${row['total_weekly_cost']:,.0f}")

def plot_pareto_frontier():
    """Plot Pareto frontier of carbon vs cost for different battery sizes"""
    
    df = calculate_total_cost_with_capex()
    
    # Setup plot
    fig, ax = plt.subplots(1, 1, figsize=(4, 4))

    strategies = df['strategy'].unique()
    colors = {'as_is': 'blue', 'only_curtail': 'red', 'carbon_aware': 'green'}
    markers = {'as_is': 'o', 'only_curtail': 's', 'carbon_aware': '^'}
    
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        
        # Plot carbon vs cost with battery capacity as sweep parameter
        x = strategy_data['total_carbon_kg']
        y = strategy_data['total_weekly_cost']
        battery_sizes = strategy_data['battery_capacity_mw']
        
        # Plot line connecting points
        ax.plot(x, y, '-', color=colors[strategy], alpha=0.7, linewidth=2)
        
        # Plot points with battery capacity labels
        for i, (carbon, cost, batt_mw) in enumerate(zip(x, y, battery_sizes)):
            ax.scatter(carbon, cost, color=colors[strategy], marker=markers[strategy], 
                      s=100, edgecolors='black', linewidth=1, zorder=5)
            
            # # Add battery capacity labels
            # if i == 0:  # First point gets strategy label
            #     ax.annotate(f'{strategy}\n{batt_mw}MW', 
            #                (carbon, cost), 
            #                xytext=(10, 10), textcoords='offset points',
            #                fontsize=9, ha='left',
            #                bbox=dict(boxstyle='round,pad=0.3', facecolor=colors[strategy], alpha=0.3))
            # else:
            ax.annotate(f'{batt_mw}MW', 
                        (carbon, cost), 
                        xytext=(5, 5), textcoords='offset points',
                        fontsize=8, ha='left')
    
    # Formatting
    ax.set_title('Pareto Frontier: Carbon vs Cost', fontsize=14, fontweight='bold')
    ax.set_xlabel('Total Carbon Emissions (kg CO2)', fontsize=12)
    ax.set_ylabel('Total Weekly Cost ($)', fontsize=12)
    ax.grid(True, alpha=0.3)
    
    # Add legend
    legend_elements = [plt.Line2D([0], [0], marker=markers[s], color=colors[s], 
                                 label=s, markersize=8, linewidth=2) 
                      for s in strategies]
    ax.legend(handles=legend_elements, loc='center left', fontsize=11)
    
    # Format axes
    ax.ticklabel_format(style='scientific', axis='both', scilimits=(0,0))
    
    plt.tight_layout()
    plt.savefig('pareto_frontier_carbon_vs_cost.pdf', dpi=150, bbox_inches='tight')
    print("Pareto frontier plot saved to 'pareto_frontier_carbon_vs_cost.pdf'")

def plot_pareto_frontier_per_job():
    """Plot Pareto frontier of carbon vs cost per job"""
    
    df = calculate_total_cost_with_capex()

    fig, ax = plt.subplots(1, 1, figsize=(5, 5))

    strategies = df['strategy'].unique()
    colors = {'as_is': 'blue', 'only_curtail': 'red', 'carbon_aware': 'green'}
    markers = {'as_is': 'o', 'only_curtail': 's', 'carbon_aware': '^'}
    
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        
        x = strategy_data['carbon_per_job']
        y = strategy_data['cost_per_job']
        battery_sizes = strategy_data['battery_capacity_mw']
        
        ax.plot(x, y, '-', color=colors[strategy], alpha=0.7, linewidth=2)
        
        for i, (carbon, cost, batt_mw) in enumerate(zip(x, y, battery_sizes)):
            ax.scatter(carbon, cost, color=colors[strategy], marker=markers[strategy], 
                      s=100, edgecolors='black', linewidth=1, zorder=5)
            
            # Only annotate non-zero battery capacities
            if batt_mw > 0:
                if strategy == 'carbon_aware':
                    ax.annotate(f'{batt_mw}MW', 
                                (carbon, cost), 
                                xytext=(-15, -10), textcoords='offset points',
                                fontsize=7, ha='center')
                else:
                    ax.annotate(f'{batt_mw}MW', 
                                (carbon, cost), 
                                xytext=(8, 8), textcoords='offset points',
                                fontsize=7, ha='left')
    
    ax.set_title('Pareto Frontier: Carbon vs Cost Per Job', fontsize=14, fontweight='bold')
    ax.set_xlabel('Carbon per Job (kg CO2)', fontsize=12)
    ax.set_ylabel('Cost per Job ($)', fontsize=12)
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.grid(True, alpha=0.3)
    
    legend_elements = [plt.Line2D([0], [0], marker=markers[s], color=colors[s], 
                                 label=s, markersize=8, linewidth=2) 
                      for s in strategies]
    ax.legend(handles=legend_elements, loc='lower left', fontsize=11)
    
    plt.tight_layout()
    plt.savefig('pareto_frontier_per_job.pdf', dpi=150, bbox_inches='tight')
    print("Per-job Pareto frontier plot saved to 'pareto_frontier_per_job.pdf'")
    
    # Print Pareto analysis
    print("\n=== Pareto Frontier Analysis ===")
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        print(f"\n{strategy} strategy:")
        for _, row in strategy_data.iterrows():
            print(f"  {row['battery_capacity_mw']} MW: ${row['total_weekly_cost']:,.0f}, {row['total_carbon_kg']:,.0f} kg CO2")

if __name__ == "__main__":
    df_results = plot_battery_impact()
    print_summary(df_results)
    
    # Plot Pareto frontiers
    plot_pareto_frontier()
    plot_pareto_frontier_per_job()
    
    # Save detailed results
    df_results.to_csv("battery_impact_analysis_detailed.csv", index=False)
    print(f"\nDetailed results saved to 'battery_impact_analysis_detailed.csv'")