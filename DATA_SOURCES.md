# Data Sources

This document describes the datasets used by the `Curtailed-2-Compute` workflow.

## Included in the Repository

The following four files are extracted from the [CAISO Revised Draft 2024-2025 Transmission Plan](https://www.caiso.com/documents/revised-draft-2024-2025-transmission-plan.pdf):

| Path | Source table | Role |
| --- | --- | --- |
| `data/caiso_wind_solar_pcm.csv` | Table 4.6-2 | Wind and solar curtailment by renewable zone (2034/2039) |
| `data/congestion_costs.csv` | Appendix G | Congestion costs and durations by constrained area |
| `data/local_generation_mix_fresno.csv` | Fresno area assessment | Resource capacity mix by type for the Fresno area |
| `data/transmission_project_costs.csv` | Project cost tables | Approved transmission project cost estimates |

Additional derived inputs:

| Path | Role |
| --- | --- |
| `data/processed/vector_high_curtailment_week.csv` | Representative 168-hour week for high-curtailment conditions |
| `data/processed/vector_high_volatility_week.csv` | Representative 168-hour week for high-price-volatility conditions |

## External Data (Download Separately)

These raw inputs should be downloaded from the original source when reproducing the full analysis:

| Path pattern | Source | Notes |
| --- | --- | --- |
| `data/productionandcurtailmentsdata_YYYY.xlsx` | [CAISO Managing Oversupply](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx) | Multi-year curtailment and production workbooks |
| `data/LMP_Data/YYYYMM_LMP.csv` | [CAISO OASIS](http://oasis.caiso.com/) | Monthly LMP exports |
| `data/wind-solar-daily-market-watch-*.xlsx` | CAISO Market Watch | Reference validation material |
| `data/curtailed-non-operational-generator-*.xlsx` | CAISO or related market reports | Supporting local review data |

## CAISO Curtailment and Production Data

- **Source**: [CAISO Managing Oversupply](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx)
- **Format**: Excel workbooks
- **Filename pattern**: `productionandcurtailmentsdata_YYYY.xlsx`
- **Temporal resolution**: 5-minute intervals
- **Expected local location**: `data/`

Each workbook contains production and curtailment information used by `notebooks/01_curtailed_energy_analysis.ipynb`.

### Key fields

| Column | Meaning |
| --- | --- |
| `Date`, `Hour`, `Interval` | Time index for each 5-minute record |
| `Solar Curtailment`, `Wind Curtailment` | Curtailment in MW for the interval |
| `Reason` | Curtailment type such as `Local` or `System` |
| `Net Load` | Demand net of wind and solar generation |

### Notes

- Convert MW to MWh for 5-minute intervals with `MW * 5 / 60`.
- Treat missing wind curtailment values as zero unless the upstream data source indicates otherwise.
- Check date coverage carefully across years before aggregating.

## CAISO LMP Data

- **Source**: [CAISO OASIS](http://oasis.caiso.com/)
- **Format**: CSV
- **Filename pattern**: `YYYYMM_LMP.csv`
- **Temporal resolution**: hourly in the current workflow
- **Expected local location**: `data/LMP_Data/`

The notebooks primarily use LMP component data to identify curtailment-aligned hours and to estimate scenario energy costs.

### Key fields

| Column | Meaning |
| --- | --- |
| `INTERVALSTARTTIME_GMT` | Interval start time |
| `NODE_ID` | Pricing node identifier |
| `XML_DATA_ITEM` | LMP component identifier |
| `MW` | Price value in dollars per MWh |

### Components used in the workflow

| `XML_DATA_ITEM` | Meaning |
| --- | --- |
| `LMP_PRC` | Total LMP |
| `LMP_ENE_PRC` | Energy component |
| `LMP_CONG_PRC` | Congestion component |
| `LMP_LOSS_PRC` | Losses component |
| `LMP_GHG_PRC` | GHG component |

The public workflow uses negative congestion values as a signal for curtailment-like conditions.

## WattTime Marginal Carbon Intensity

- **Source**: [WattTime API](https://watttime.org/)
- **Used by**: `notebooks/03_lmp_vectors.ipynb`
- **Role**: Provides hourly marginal carbon intensity (`marginal_co2_lbs_per_mwh`) for the CAISO NP-15 node, used to build the carbon column in the representative weekly vectors.
- **Access**: Requires a free WattTime account. Set `WATTTIME_USER` and `WATTTIME_PASSWORD` environment variables before running notebook 03.
- **Note**: If you are using the **lightweight path** (notebooks 04-05 or the `eval/` workflow), this data is already included in the pre-built vectors. WattTime credentials are only needed for full end-to-end reproduction.

## CAISO 2024-2025 Transmission Plan Data

The four supporting CSV files listed above were extracted from the [CAISO Revised Draft 2024-2025 Transmission Plan](https://www.caiso.com/documents/revised-draft-2024-2025-transmission-plan.pdf). They provide transmission zone curtailment projections, congestion cost estimates, Fresno-area generation capacity, and approved project costs used in the TAC and economic analysis notebooks.

## Data Validation Checklist

Before running or publishing results from raw inputs:

1. Verify that the expected years and months are present.
2. Check for duplicate timestamps after timezone conversion.
3. Confirm that LMP components can be reconciled to total LMP.
4. Confirm that curtailment values are non-negative after cleaning.
5. Document any filtering, interpolation, or aggregation steps that affect published figures.

## Workflow Mapping

| Notebook | Primary data inputs |
| --- | --- |
| `notebooks/01_curtailed_energy_analysis.ipynb` | Raw CAISO curtailment workbooks |
| `notebooks/02_TAC.ipynb` | Outputs from notebook 01 plus TAC-zone mapping inputs |
| `notebooks/03_lmp_vectors.ipynb` | Curtailment outputs plus raw monthly LMP CSVs |
| `notebooks/04_electricity_costs_analysis.ipynb` | LMP-derived vectors and tariff assumptions |
| `notebooks/05_scenario_financial_analysis.ipynb` | Scenario cost results and model assumptions |

