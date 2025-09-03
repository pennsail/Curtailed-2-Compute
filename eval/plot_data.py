import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Load both datasets
df1 = pd.read_csv('vector_high_volatility_week_v2.csv')
df1['datetime'] = pd.to_datetime(df1['datetime'])

df2 = pd.read_csv('vector_high_curtailment_week_v2.csv')
df2['datetime'] = pd.to_datetime(df2['datetime'])

# Create 3x2 subplots
fig, axes = plt.subplots(3, 2, figsize=(15, 12))

# Column 1: High Volatility Week
axes[0,0].plot(df1['datetime'], df1['LMP_NP15'], 'b-', linewidth=1)
axes[0,0].set_ylabel('Price ($/MWh)')
axes[0,0].set_title('High Volatility - Price')
axes[0,0].grid(True, alpha=0.3)

axes[1,0].plot(df1['datetime'], df1['Total_Curtailment_NP15_MW'], 'r-', linewidth=1)
axes[1,0].set_ylabel('Curtailment (MW)')
axes[1,0].set_title('High Volatility - Curtailment')
axes[1,0].grid(True, alpha=0.3)

axes[2,0].plot(df1['datetime'], df1['marginal_co2_lbs_per_mwh'], 'g-', linewidth=1)
axes[2,0].set_ylabel('Carbon (lbs CO₂/MWh)')
axes[2,0].set_title('High Volatility - Carbon Intensity')
axes[2,0].grid(True, alpha=0.3)

# Column 2: High Curtailment Week
axes[0,1].plot(df2['datetime'], df2['LMP_NP15'], 'b-', linewidth=1)
axes[0,1].set_ylabel('Price ($/MWh)')
axes[0,1].set_title('High Curtailment - Price')
axes[0,1].grid(True, alpha=0.3)

axes[1,1].plot(df2['datetime'], df2['Total_Curtailment_NP15_MW'], 'r-', linewidth=1)
axes[1,1].set_ylabel('Curtailment (MW)')
axes[1,1].set_title('High Curtailment - Curtailment')
axes[1,1].grid(True, alpha=0.3)

axes[2,1].plot(df2['datetime'], df2['marginal_co2_lbs_per_mwh'], 'g-', linewidth=1)
axes[2,1].set_ylabel('Carbon (lbs CO₂/MWh)')
axes[2,1].set_title('High Curtailment - Carbon Intensity')
axes[2,1].grid(True, alpha=0.3)

# Format x-axis for both columns
for row in range(3):
    for col in range(2):
        axes[row,col].xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
        axes[row,col].xaxis.set_major_locator(mdates.DayLocator())
        plt.setp(axes[row,col].xaxis.get_majorticklabels(), rotation=45)

plt.tight_layout()
plt.savefig('data_comparison.png', dpi=150, bbox_inches='tight')
print("Comparison plot saved as data_comparison.png")