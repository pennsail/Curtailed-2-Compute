import pandas as pd
import matplotlib.pyplot as plt

def plot_curtail_only_goodput_cost():
    """Plot job goodput and total cost vs battery capacity for curtail only strategy"""
    
    # Load data
    df = pd.read_csv('battery_analysis_results_high_curtailment.csv')
    curtail_only = df[df['strategy'] == 'only_curtail'].sort_values('battery_capacity_mw')
    
    fig, ax1 = plt.subplots(figsize=(5, 4))
    
    # Plot job goodput on left y-axis
    color = 'tab:blue'
    ax1.set_xlabel('Battery Capacity (MW)')
    ax1.set_ylabel('Total Jobs Scheduled', color=color)
    line1 = ax1.plot(curtail_only['battery_capacity_mw'], curtail_only['total_jobs_scheduled'], 
             'o-', color=color, linewidth=2, markersize=6, label='Jobs Scheduled')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.grid(True, alpha=0.3)
    
    # Create second y-axis for total cost
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel('Total Cost ($k)', color=color)
    line2 = ax2.plot(curtail_only['battery_capacity_mw'], curtail_only['total_cost_usd']/1000, 
             's-', color=color, linewidth=2, markersize=6, label='Total Cost')
    ax2.tick_params(axis='y', labelcolor=color)
    
    # Add legend
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc='lower right')
    
    plt.title('Curtail Only: Job Goodput and Cost vs Battery Capacity')
    plt.tight_layout()
    plt.savefig('curtail_only_goodput_cost.pdf', dpi=150, bbox_inches='tight')
    print("Curtail only analysis saved to 'curtail_only_goodput_cost.pdf'")

if __name__ == "__main__":
    plot_curtail_only_goodput_cost()