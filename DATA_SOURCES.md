# Data Sources Documentation

This document provides comprehensive documentation for all data sources used in the Curtailed-2-Compute project.

## CAISO Data Sources

### 1. Curtailment and Production Data

**Source**: [CAISO Managing Oversupply Reports](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx)

**File Format**: Excel (.xlsx)

**File Naming Convention**: `productionandcurtailmentsdata_YYYY.xlsx` (e.g., `productionandcurtailmentsdata_2024.xlsx`)

**Temporal Resolution**: 5-minute intervals

**Coverage**: 2020-2025 (as available)

**File Location**: Source data files are archived in `misc/archived_files/` to keep code directories clean. When running analyses, you may need to move these files to `data/` or update notebook paths.

**Download Instructions**:
1. Visit the CAISO Managing Oversupply page
2. Navigate to "Production and Curtailments Data"
3. Download files for desired years
4. Place files in `misc/archived_files/` (or `data/` if you prefer to keep them with other data files)

#### Sheet 1: Production

This sheet contains real-time energy supply and demand data recorded in 5-minute intervals.

| Column | Description | Units | Notes |
|--------|-------------|-------|-------|
| `Date` | Date of the record | YYYY-MM-DD | |
| `Hour` | Hour of the day | 1-24 | 24-hour format |
| `Interval` | 5-minute interval within the hour | 1-12 | 12 intervals per hour |
| `Load` | Actual electricity demand | MW | Total system load |
| `Net Load` | Load minus wind and solar production | MW | Represents demand that must be met by dispatchable resources |
| `Solar` | Solar generation at that interval | MW | Utility-scale solar |
| `Wind` | Wind generation at that interval | MW | Utility-scale wind |
| `Renewables` | Total renewable production | MW | Includes solar, wind, biomass, geothermal, small hydro |
| `Thermal` | Generation from natural gas and other thermal resources | MW | Excludes nuclear |
| `Nuclear` | Nuclear generation | MW | |
| `Large Hydro` | Large-scale hydropower generation | MW | Not included in `Renewables` |
| `Imports` | Electricity imported into the CAISO grid | MW | |
| `Generation` | Total internal generation | MW | |
| `Load Less (Generation+Imports)` | Imbalance between demand and supply | MW | Sanity check (should be near zero) |

**Key Insights**:
- `Net Load` shows the "duck curve" pattern, particularly in spring months
- Negative `Net Load` values indicate periods of renewable oversupply
- `Load Less (Generation+Imports)` should be near zero; large deviations may indicate data quality issues

#### Sheet 2: Curtailments

This sheet provides records of curtailed solar and wind generation—clean energy that could not be delivered to the grid.

| Column | Description | Units | Notes |
|--------|-------------|-------|-------|
| `Date` | Date of curtailment | YYYY-MM-DD | |
| `Hour` | Hour of the day | 1-24 | 24-hour format |
| `Interval` | 5-minute interval | 1-12 | |
| `Wind Curtailment` | Wind energy curtailed in that interval | MW | May be NaN (treat as 0) |
| `Solar Curtailment` | Solar energy curtailed in that interval | MW | |
| `Reason` | Type of curtailment event | Text | `Local`: Localized grid constraints<br>`System`: System-wide oversupply |

**Curtailment Definition**:
Curtailment is defined as the difference between actual production and the forecast when actual production is less than the forecast. Only wind and solar resources are reported in this manner because these resources have forecasts. Other renewables (geothermal, biomass) are baseload and rarely curtailed.

**Energy Conversion**:
To convert MW to MWh for a 5-minute interval:
```
MWh = MW × (5 minutes / 60 minutes) = MW × 0.0833
```

**Curtailment Reasons**:
- **Local**: Due to localized grid constraints, such as transmission congestion or limited load in a specific area
- **System**: Due to system-wide oversupply, meaning total renewable generation across the grid exceeded total demand

### 2. Locational Marginal Price (LMP) Data

**Source**: [CAISO OASIS](http://oasis.caiso.com/)

**File Format**: CSV

**File Naming Convention**: `YYYYMM_LMP.csv` (e.g., `202401_LMP.csv`)

**Temporal Resolution**: Hourly (with some 5-minute data available)

**Coverage**: Available for download by month

**Download Instructions**:
1. Visit CAISO OASIS
2. Navigate to "Reports" → "Locational Marginal Price"
3. Select desired node/location and time period
4. Download CSV files
5. Place files in `data/LMP_Data/` directory

#### LMP Data Structure

| Column | Description | Units | Notes |
|--------|-------------|-------|-------|
| `INTERVALSTARTTIME_GMT` | Start time of interval | ISO 8601 | GMT timezone |
| `NODE_ID` | Pricing node identifier | Text | Example: `TH_NP15_GEN-APND` |
| `XML_DATA_ITEM` | LMP component type | Text | See below |
| `MW` | Price value | $/MWh | |

**LMP Components** (XML_DATA_ITEM values):

| Component | Description | Typical Range | Notes |
|-----------|-------------|---------------|-------|
| `LMP_PRC` | Total LMP | Variable | Sum of all components |
| `LMP_ENE_PRC` | Energy component | $20-100/MWh | Base energy price |
| `LMP_CONG_PRC` | Congestion component | -$50 to +$50/MWh | **Negative values indicate curtailment** |
| `LMP_LOSS_PRC` | Losses component | $0-5/MWh | Transmission losses |
| `LMP_GHG_PRC` | GHG component | $0-30/MWh | Greenhouse gas adder |

**Key Insights**:
- **Negative congestion component** (`LMP_CONG_PRC < 0`) indicates curtailment conditions
- Total LMP = Energy + Congestion + Losses + GHG
- LMP varies by location (node); rural nodes may have different patterns than metro nodes

**Node Selection**:
- `TH_NP15_GEN-APND`: Northern California pricing node (used in analyses)
- Other nodes available for location-specific analysis

### 3. Wind and Solar Daily Market Watch Data

**Source**: CAISO Market Watch

**File Format**: Excel (.xlsx)

**Coverage**: Daily snapshots

**Purpose**: Additional validation and trend analysis

## Derived and Processed Datasets

### 1. Aggregated Curtailment Data

**Location**: `outputs/`

**Files**:
- `daily_curtailment_breakdown.csv`
- `monthly_curtailment_breakdown.csv`
- `yearly_curtailment_breakdown.csv`

**Description**: Pre-aggregated curtailment statistics by time period

### 2. Merged Data Center and Curtailment Data

**Location**: `data/Merged_Data_Center_and_Curtailment_Data.csv`

**Description**: Combined data center location/capacity data with curtailment patterns

### 3. Carbon Intensity Data

**Location**: `notebooks/data/`

**Files**:
- `carbon_intensity_high_curtailment_week.csv`
- `carbon_intensity_high_volatility_week.csv`

**Description**: Carbon intensity calculations for specific time periods

## External Data Sources

### 1. Data Center Information

**Sources**:
- Industry reports and surveys
- Public data center registries
- Market research

**Files**:
- `data/ca_data_centers_curtailed_energy.csv`
- `data/agg_electricity_total.csv`

### 2. Transmission and Grid Data

**Files**:
- `data/transmission_project_costs.csv`
- `data/congestion_costs.csv`
- `data/local_generation_mix_fresno.csv`

### 3. Reference Documents

**Location**: `data/` and `misc/`

**Files**:
- Various PDF reports from CAISO, transmission planning documents
- Industry white papers
- Regulatory documents

## Data Quality Notes

### Known Issues

1. **Missing Wind Curtailment Data**: Some intervals may have NaN values for wind curtailment. These should be treated as 0 MW.

2. **Time Zone Handling**: 
   - CAISO data is typically in Pacific Time
   - LMP data may be in GMT; conversion to Pacific Time is required
   - Daylight saving time transitions should be handled carefully

3. **Data Gaps**: Some months or days may have incomplete data. Check for gaps before analysis.

### Data Validation

Recommended checks before analysis:
- Verify date ranges are complete
- Check for duplicate timestamps
- Validate that curtailment values are non-negative
- Ensure LMP components sum to total LMP
- Verify time zone conversions are correct

## Data Processing Workflows

### Loading Curtailment Data

See `notebooks/01_curtailed_energy_analysis.ipynb` for example code:

```python
import pandas as pd
from pathlib import Path

data_dir = Path("data")
years = [2020, 2021, 2022, 2023, 2024, 2025]
combined_df = []

for year in years:
    file = data_dir / f"productionandcurtailmentsdata_{year}.xlsx"
    xl = pd.ExcelFile(file)
    df = xl.parse("Curtailments")
    df['Year'] = year
    combined_df.append(df)

curtailment_df = pd.concat(combined_df, ignore_index=True)
```

### Loading LMP Data

See `notebooks/electricity_costs_analysis.ipynb` for example code:

```python
import glob
import pandas as pd

all_files = glob.glob("data/LMP_Data/2024*_LMP.csv")
df_list = []

for file in all_files:
    df = pd.read_csv(file)
    df = df[df['NODE_ID'] == 'TH_NP15_GEN-APND'].copy()
    # Process and pivot data
    df_list.append(df)

final_df = pd.concat(df_list, ignore_index=True)
```

## Data Updates

### How to Update Data

1. **Curtailment Data**: Download new monthly/yearly files from CAISO and add to `data/` directory
2. **LMP Data**: Download monthly files and add to `data/LMP_Data/` directory
3. **Update Notebooks**: Modify file paths and date ranges in notebooks as needed

### Data Retention

- Keep original downloaded files in `data/` directory
- Processed/aggregated data should be in `outputs/` directory
- Consider archiving older data if storage becomes an issue

## References

- [CAISO Managing Oversupply](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx)
- [CAISO OASIS](http://oasis.caiso.com/)
- [CAISO Market Watch](http://www.caiso.com/market/Pages/MarketWatch.aspx)

## Questions or Issues

If you encounter data quality issues or have questions about data sources, please:
1. Check this documentation first
2. Review the relevant notebook for processing examples
3. Open an issue in the repository (if applicable)

