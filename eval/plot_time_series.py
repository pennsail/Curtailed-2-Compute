import pandas as pd
import matplotlib.pyplot as plt

# Load data
high_curtailment = pd.read_csv('vector_high_curtailment_week_v2.csv')
high_volatility = pd.read_csv('vector_high_volatility_week_v2.csv')
high_curtailment['datetime'] = pd.to_datetime(high_curtailment['datetime'])
high_volatility['datetime'] = pd.to_datetime(high_volatility['datetime'])

# Create relative time for visualization
high_curtailment['hours'] = range(len(high_curtailment))
high_volatility['hours'] = range(len(high_volatility), len(high_volatility) + len(high_volatility))

# Plot
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

# LMP comparison
ax1.plot(high_curtailment['hours'], high_curtailment['LMP_NP15'], label='High Curtailment Week')
ax1.plot(high_volatility['hours'], high_volatility['LMP_NP15'], label='High Volatility Week')
ax1.set_title('LMP Comparison')
ax1.set_ylabel('LMP ($/MWh)')
ax1.legend()
ax1.grid(True)

# Curtailment comparison
ax2.plot(high_curtailment['hours'], high_curtailment['Total_Curtailment_NP15_MW'], label='High Curtailment Week')
ax2.plot(high_volatility['hours'], high_volatility['Total_Curtailment_NP15_MW'], label='High Volatility Week')
ax2.set_title('Curtailment Comparison')
ax2.set_ylabel('Curtailment (MW)')
ax2.set_xlabel('Hours')
ax2.legend()
ax2.grid(True)

plt.tight_layout()
plt.savefig('high_curtailment_volatility_week.png', dpi=150, bbox_inches='tight')
print("Charts saved to 'high_curtailment_volatility_week.png'.")