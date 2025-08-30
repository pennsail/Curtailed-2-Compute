import pandas as pd
import matplotlib.pyplot as plt

# Load data
high_curtailment = pd.read_csv('vector_high_curtailment_week.csv')
high_volatility = pd.read_csv('vector_high_volatility_week.csv')

# Plot
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8))

# High curtailment week - plot curtailed MW
ax1.plot(high_curtailment['hour'], high_curtailment['curtailed_mw'])
ax1.set_title('High Curtailment Week - Curtailed Energy')
ax1.set_ylabel('Curtailed MW')
ax1.grid(True)

# High volatility week - plot electricity prices
ax2.plot(high_volatility['hour'], high_volatility['lmp_$/mwh'])
ax2.set_title('High Volatility Week - Electricity Prices')
ax2.set_ylabel('LMP ($/MWh)')
ax2.set_xlabel('Hour')

plt.tight_layout()
plt.savefig('high_curtailment_volatility_week.png', dpi=150, bbox_inches='tight')
print("Charts saved to 'high_curtailment_volatility_week.png'.")