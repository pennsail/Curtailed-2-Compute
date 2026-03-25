# Workflow Guide

The notebook sequence runs in order:

```text
01 -> 02 -> 03 -> 04 -> 05
```

## Step 1: `notebooks/01_curtailed_energy_analysis.ipynb`

**Purpose**

- Load multi-year CAISO curtailment workbooks
- Clean and aggregate curtailment observations
- Build daily, monthly, seasonal, and yearly summaries

**Primary inputs**

- `data/productionandcurtailmentsdata_YYYY.xlsx`

**Typical outputs**

- plots and diagnostics for curtailment patterns

## Step 2: `notebooks/02_TAC.ipynb`

**Purpose**

- map curtailment results into TAC-zone views
- support location-sensitive interpretation of congestion and curtailment exposure

**Primary inputs**

- outputs from notebook 01
- TAC mapping assumptions used in the notebook

## Step 3: `notebooks/03_lmp_vectors.ipynb`

**Purpose**

- combine curtailment analysis with monthly LMP data
- identify representative weeks for later economic analysis
- build reusable vectors for high-curtailment and high-volatility cases

**Primary inputs**

- outputs from notebooks 01 and 02
- `data/LMP_Data/YYYYMM_LMP.csv`

**Canonical derived outputs**

- `data/processed/vector_high_curtailment_week.csv`
- `data/processed/vector_high_volatility_week.csv`

## Step 4: `notebooks/04_electricity_costs_analysis.ipynb`

**Purpose**

- estimate electricity costs under the three scenario families
- compare metro, rural-flexible, and rural-plus-battery operating cases

**Primary inputs**

- LMP-derived vectors and tariff assumptions

**Scenario framing**

- **Scenario A**: metro baseline
- **Scenario B**: rural flexible load
- **Scenario C**: rural flexible load with battery storage

## Step 5: `notebooks/05_scenario_financial_analysis.ipynb`

**Purpose**

- run the parameterized economic model
- calculate NPV, IRR, MIRR, and minimum viable rent
- perform scenario comparison and sensitivity analysis

**Primary inputs**

- cost outputs and assumptions from the prior notebooks

## Dependency Summary

| Notebook | Depends on |
| --- | --- |
| `01_curtailed_energy_analysis.ipynb` | Raw CAISO curtailment workbooks |
| `02_TAC.ipynb` | Notebook 01 results |
| `03_lmp_vectors.ipynb` | Notebooks 01 and 02 plus monthly LMP files |
| `04_electricity_costs_analysis.ipynb` | Representative vectors and scenario assumptions |
| `05_scenario_financial_analysis.ipynb` | Scenario cost results and financial assumptions |

