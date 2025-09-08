import pandas as pd
import matplotlib.pyplot as plt

def plot_battery_impact_from_csv():
    """Create 1x2 plot showing battery impact analysis from CSV"""
    
    df = pd.read_csv("battery_impact_analysis_detailed_annual.csv")
    
    # Setup plot - 1 row, 2 columns
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7, 3))
    
    strategies = df['strategy'].unique()
    colors = {'as_is': '#2E86AB', 'only_curtail': '#A23B72', 'carbon_aware': '#F18F01'}
    
    # Left plot: Total Cost (left y-axis) and Carbon (right y-axis)
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        ax1.plot(strategy_data['battery_capacity_mw'], strategy_data['total_weekly_cost']/1000, 
                'o-', color=colors[strategy], label=f'Cost of\n{strategy}', linewidth=2, markersize=4, alpha=0.8)

    ax1.set_xlabel('Battery Capacity (MW)')
    ax1.set_ylabel('Total Weekly Cost ($k)', color='black')
    ax1.tick_params(axis='y', labelcolor='black')
    ax1.grid(True, alpha=0.3)
    
    # Create second y-axis for carbon
    ax1_twin = ax1.twinx()
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        ax1_twin.plot(strategy_data['battery_capacity_mw'], strategy_data['total_carbon_kg'],
                     's--', color=colors[strategy], label=f'Emissions of\n{strategy}', linewidth=2, markersize=4, alpha=0.8)

    ax1_twin.set_ylabel('Total Emissions (kg CO2)', color='gray')
    ax1_twin.tick_params(axis='y', labelcolor='gray')

    ax1.set_title('Total Cost & Emissions vs Battery Size')
    
    # Right plot: Cost per Job (left y-axis) and Carbon per Job (right y-axis)
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        ax2.plot(strategy_data['battery_capacity_mw'], strategy_data['cost_per_job'],
                'o-', color=colors[strategy], label=f'Cost of\n{strategy}', linewidth=2, markersize=4, alpha=0.8)

    ax2.set_xlabel('Battery Capacity (MW)')
    ax2.set_ylabel('Cost per Job ($)', color='black')
    ax2.tick_params(axis='y', labelcolor='black')
    ax2.grid(True, alpha=0.3)
    
    # Create second y-axis for carbon per job
    ax2_twin = ax2.twinx()
    for strategy in strategies:
        strategy_data = df[df['strategy'] == strategy].sort_values('battery_capacity_mw')
        ax2_twin.plot(strategy_data['battery_capacity_mw'], strategy_data['carbon_per_job'],
                     's--', color=colors[strategy], label=f'Emissions of\n{strategy}', linewidth=2, markersize=4, alpha=0.8)

    ax2_twin.set_ylabel('Emissions per Job (kg CO2)', color='gray')
    ax2_twin.tick_params(axis='y', labelcolor='gray')

    ax2.set_title('Per-Job Cost & Emissions vs Battery Size')

    # Create combined legend outside the plots
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1_twin.get_legend_handles_labels()
    fig.legend(lines1 + lines2, labels1 + labels2, loc='center', bbox_to_anchor=(0.5, -0.08), ncol=3)
    
    plt.tight_layout()
    plt.savefig('battery_impact_analysis.pdf', dpi=150, bbox_inches='tight')
    print("Battery impact analysis saved to 'battery_impact_analysis.pdf'")

if __name__ == "__main__":
    plot_battery_impact_from_csv()