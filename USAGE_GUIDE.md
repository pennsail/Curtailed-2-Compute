# CAISO Data Usage Guide

This guide provides step-by-step instructions for working with CAISO data in the Curtailed-2-Compute project.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Downloading CAISO Data](#downloading-caiso-data)
3. [Data Processing Workflows](#data-processing-workflows)
4. [Common Analyses](#common-analyses)
5. [Troubleshooting](#troubleshooting)

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Jupyter Notebook or JupyterLab
- Required Python packages (install via `pip install -r requirements.txt`)

### Repository Structure

```
Curtailed-2-Compute/
├── data/                    # Place downloaded data files here
│   ├── LMP_Data/           # Locational Marginal Price data
│   └── [curtailment files]  # Production and curtailment Excel files
├── notebooks/               # Analysis notebooks
└── outputs/                 # Generated results and visualizations
```

## Downloading CAISO Data

### 1. Curtailment and Production Data

**Source**: [CAISO Managing Oversupply](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx)

**Steps**:
1. Navigate to the CAISO Managing Oversupply page
2. Scroll to "Production and Curtailments Data"
3. Download Excel files for desired years (format: `productionandcurtailmentsdata_YYYY.xlsx`)
4. Place files in `misc/archived_files/` (or `data/` if you prefer)

**Note**: Source data files (XLSX) are stored in `misc/archived_files/` to keep code directories clean. Update notebook paths accordingly or move files to `data/` when needed.

**File Format**:
- Each file contains two sheets: `Production` and `Curtailments`
- Data is at 5-minute intervals
- See [DATA_SOURCES.md](DATA_SOURCES.md) for detailed field descriptions

**Example**:
```python
from pathlib import Path
import pandas as pd

data_dir = Path("data")
file = data_dir / "productionandcurtailmentsdata_2024.xlsx"

# Load curtailment data
xl = pd.ExcelFile(file)
curtailment_df = xl.parse("Curtailments")
production_df = xl.parse("Production")
```

### 2. Locational Marginal Price (LMP) Data

**Source**: [CAISO OASIS](http://oasis.caiso.com/)

**Steps**:
1. Visit CAISO OASIS
2. Navigate to "Reports" → "Locational Marginal Price"
3. Select:
   - **Node**: Choose pricing node (e.g., `TH_NP15_GEN-APND` for Northern California)
   - **Time Period**: Select desired month/year
   - **Data Item**: Select all LMP components
4. Download CSV files
5. Place files in `data/LMP_Data/` directory

**File Naming**: Use format `YYYYMM_LMP.csv` (e.g., `202401_LMP.csv`)

**Example**:
```python
import glob
import pandas as pd

# Load all monthly LMP files
all_files = glob.glob("data/LMP_Data/2024*_LMP.csv")
df_list = []

for file in all_files:
    df = pd.read_csv(file)
    # Filter for specific node
    df = df[df['NODE_ID'] == 'TH_NP15_GEN-APND'].copy()
    df_list.append(df)

lmp_df = pd.concat(df_list, ignore_index=True)
```

## Data Processing Workflows

### Workflow 1: Analyzing Curtailment Patterns

**Notebook**: `notebooks/01_curtailed_energy_analysis.ipynb`

**Steps**:
1. Load multiple years of curtailment data
2. Clean and combine data
3. Convert MW to MWh (5-minute intervals: MW × 5/60)
4. Aggregate by time period (daily, monthly, yearly)
5. Visualize trends

**Key Code Snippet**:
```python
# Convert MW to MWh for 5-minute intervals
curtailment_df['Solar Curtailment (MWh)'] = (
    curtailment_df['Solar Curtailment'] * 5 / 60
)

# Aggregate by month
monthly_curtailment = curtailment_df.groupby('Month')[
    ['Solar Curtailment (MWh)', 'Wind Curtailment (MWh)']
].sum()
```

### Workflow 2: Identifying Curtailment Hours from LMP Data

**Notebook**: `notebooks/electricity_costs_analysis.ipynb`

**Methodology**: 
- Negative congestion component (`LMP_CONG_PRC < 0`) indicates curtailment conditions
- This captures both local and system-wide curtailment events

**Steps**:
1. Load LMP data for desired time period
2. Pivot data to separate LMP components
3. Convert timezone from GMT to Pacific Time
4. Identify curtailment hours
5. Calculate costs for different scenarios

**Key Code Snippet**:
```python
# Identify curtailment hours
df['is_curtailment'] = df['lmp_congestion'] <= 0

# Calculate energy cost during curtailment
df['energy_cost'] = df.apply(
    lambda x: (load_mw * 1000) * x['total_lmp'] / 1000 
    if x['is_curtailment'] 
    else (load_mw * 1000) * tariff_rate,
    axis=1
)
```

### Workflow 3: Financial Analysis

**Notebook**: `npv_analysis/datacenter_NPV_analysis.ipynb`

**Steps**:
1. Define scenario parameters (CAPEX, OPEX, revenue)
2. Calculate cash flows over project lifetime
3. Compute NPV, IRR, and payback period
4. Perform sensitivity analysis

**Key Code Snippet**:
```python
import numpy_financial as npf

# Calculate NPV
cash_flows = [-initial_capex] + annual_cash_flows
npv = npf.npv(discount_rate, cash_flows)
irr = npf.irr(cash_flows)
```

## Common Analyses

### Analysis 1: Daily Curtailment Trends

**Purpose**: Understand day-to-day variation in curtailment

```python
# Aggregate by date
daily_curtailment = curtailment_df.groupby('Date')[
    ['Solar Curtailment (MWh)', 'Wind Curtailment (MWh)']
].sum()

# Visualize
import matplotlib.pyplot as plt
daily_curtailment.plot(figsize=(15, 5))
plt.title("Daily Curtailment Trends")
plt.ylabel("Curtailment (MWh)")
plt.show()
```

### Analysis 2: Seasonal Patterns (Duck Curve)

**Purpose**: Identify seasonal and hourly patterns

```python
# Add month and hour columns
df['Month'] = df['Datetime'].dt.month
df['Hour'] = df['Datetime'].dt.hour

# Average by hour for spring months
spring_hours = df[df['Month'].isin([3, 4, 5])].groupby('Hour')[
    'Solar Curtailment (MWh)'
].mean()

spring_hours.plot(kind='line')
plt.title("Average Hourly Solar Curtailment (Spring)")
plt.xlabel("Hour of Day")
plt.ylabel("Curtailment (MWh)")
plt.show()
```

### Analysis 3: Cost Comparison Across Scenarios

**Purpose**: Compare electricity costs for different deployment scenarios

```python
# Scenario A: Traditional (no flexibility)
cost_a = calculate_traditional_cost(df, tariff_rates)

# Scenario B: Flexible load (can use curtailed energy)
cost_b = calculate_flexible_cost(df, tariff_rates)

# Scenario C: With battery storage
cost_c = calculate_battery_scenario(df, tariff_rates, battery_params)

# Compare
comparison = pd.DataFrame({
    'Scenario A': cost_a,
    'Scenario B': cost_b,
    'Scenario C': cost_c
}, index=['Energy Cost', 'Demand Charge', 'Total'])
```

## Troubleshooting

### Issue 1: Missing Data

**Problem**: Some intervals have NaN values for wind curtailment

**Solution**:
```python
# Fill NaN values with 0
curtailment_df['Wind Curtailment'] = curtailment_df['Wind Curtailment'].fillna(0)
```

### Issue 2: Timezone Issues

**Problem**: LMP data is in GMT, need Pacific Time

**Solution**:
```python
# Convert to datetime and set timezone
df['datetime'] = pd.to_datetime(df['INTERVALSTARTTIME_GMT'])
df = df.set_index('datetime')
df = df.tz_convert('America/Los_Angeles')
```

### Issue 3: File Not Found Errors

**Problem**: Notebook can't find data files

**Solution**:
- Check that files are in the correct directory (`data/` or `data/LMP_Data/`)
- Verify file naming matches expected patterns
- Use absolute paths if running from different directories:
```python
from pathlib import Path
data_dir = Path(__file__).parent.parent / "data"
```

### Issue 4: Memory Issues with Large Datasets

**Problem**: Running out of memory when loading multiple years

**Solution**:
- Load and process one year at a time
- Use chunking for large files:
```python
# Process in chunks
chunk_size = 10000
for chunk in pd.read_csv(file, chunksize=chunk_size):
    # Process chunk
    process_chunk(chunk)
```

### Issue 5: Incorrect LMP Component Sums

**Problem**: LMP components don't sum to total LMP

**Solution**:
- Verify data quality
- Check for missing values
- Validate with CAISO documentation:
```python
# Validate LMP components
df['calculated_total'] = (
    df['lmp_energy'] + 
    df['lmp_congestion'] + 
    df['lmp_losses'] + 
    df['lmp_ghg']
)
df['difference'] = abs(df['total_lmp'] - df['calculated_total'])
# Flag large differences
large_diff = df[df['difference'] > 0.01]
```

## Best Practices

1. **Data Validation**: Always check for missing values, duplicates, and data quality issues before analysis
2. **Time Zone Handling**: Be explicit about time zones; convert to Pacific Time for consistency
3. **Unit Conversions**: Remember to convert MW to MWh for 5-minute intervals (multiply by 5/60)
4. **Documentation**: Add comments explaining methodology, especially for curtailment identification
5. **Version Control**: Keep original downloaded files; save processed data separately
6. **Reproducibility**: Use consistent date ranges and parameters across analyses

## Additional Resources

- [CAISO Managing Oversupply](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx)
- [CAISO OASIS Documentation](http://oasis.caiso.com/)
- [DATA_SOURCES.md](DATA_SOURCES.md) - Detailed data source documentation
- [README.md](README.md) - Project overview

## Getting Help

If you encounter issues:
1. Check this guide and [DATA_SOURCES.md](DATA_SOURCES.md)
2. Review relevant notebooks for examples
3. Check CAISO documentation for data format changes
4. Open an issue in the repository (if applicable)

