# Analysis Workflow Guide

This document describes the numbered workflow sequence for the Curtailed-2-Compute analysis.

## Workflow Overview

The analysis follows a sequential workflow from Notebook 01 through Notebook 05:

```
01 → 02 → 03 → 04 → 05
```

Each notebook builds on the previous one, creating a complete analysis pipeline from raw data to financial modeling.

## Workflow Steps

### Step 1: `01_curtailed_energy_analysis.ipynb`
**Purpose**: Examine curtailed renewable energy patterns

**What it does**:
- Loads and processes CAISO curtailment Excel files
- Analyzes temporal patterns (daily, monthly, yearly)
- Identifies seasonal variations (duck curve patterns)
- Distinguishes local vs. system-wide curtailment events

**Outputs**:
- Aggregated curtailment data (daily, monthly, yearly)
- Visualization plots
- Statistical summaries

**Next step**: Provides processed curtailment data for TAC zone analysis

---

### Step 2: `02_TAC.ipynb`
**Purpose**: Convert data by TAC (Transmission Access Charge) zones

**What it does**:
- Maps curtailment data to TAC zones from CAISO transmission plan report
- Analyzes curtailment patterns by transmission zone
- Prepares zone-specific data for downstream analysis

**Why TAC zones matter**:
- TAC zones are used for cost allocation in the CAISO grid
- Help identify which utility areas absorb or shed excess renewables
- Support understanding of local vs. system-wide curtailment patterns
- Important for transmission planning and congestion analysis

**Outputs**:
- TAC zone-mapped curtailment data
- Zone-specific analysis results

**Next step**: Provides zone data for LMP vector creation

---

### Step 3: `03_lmp_vectors.ipynb`
**Purpose**: Create LMP vectors for representative weeks

**What it does**:
- Integrates curtailment data with Locational Marginal Price (LMP) data
- Identifies representative weeks:
  - Week with highest curtailment
  - Week with highest LMP volatility
- Creates 168-hour (weekly) vectors combining curtailment and LMP data
- Optionally adds carbon intensity data from WattTime API
- Generates monthly representative vectors

**Outputs**:
- `vector_high_curtailment_week.csv` - 168-hour vector for highest curtailment week
- `vector_high_volatility_week.csv` - 168-hour vector for highest volatility week
- `monthly_representative_vectors.xlsx` - Monthly representative vectors

**Next step**: Provides vectors for cost analysis

---

### Step 4: `04_electricity_costs_analysis.ipynb`
**Purpose**: Analyze electricity costs for three deployment scenarios

**What it does**:
- Identifies curtailment hours using LMP congestion component
- Calculates annual electricity costs for three scenarios:
  - **Scenario A**: Metro Baseline (traditional, no flexibility)
  - **Scenario B**: Rural Flexible Load (can shift to curtailment hours)
  - **Scenario C**: Rural with Battery Storage (BESS + flexible load)
- Compares costs across scenarios
- Calculates savings from flexible load and battery storage

**Key Methodology**:
- Curtailment identification: `lmp_congestion <= 0`
- Energy charges: PG&E B-20 tariff structure (TOU rates)
- Demand charges: Based on peak demand
- Curtailed energy: Priced at total LMP (typically lower)

**Outputs**:
- Annual cost comparisons
- Savings calculations
- LMP trend visualizations

**Next step**: Provides cost analysis for financial modeling

---

### Step 5: `05_scenario_financial_analysis.ipynb`
**Purpose**: Comprehensive financial analysis (NPV, IRR, minimum viable rent)

**What it does**:
- Calculates financial metrics for all three scenarios:
  - Net Present Value (NPV)
  - Internal Rate of Return (IRR) with robust fallback methods
  - Modified IRR (MIRR)
  - Total Cost of Ownership (TCO)
  - Discounted Payback Period
- Determines minimum viable rent for target IRR (15%)
- Performs bankability screening
- Creates scenario comparison tables

**Key Features**:
- Robust IRR calculation (numpy_financial → bisection → MIRR fallback)
- Multi-component revenue model (base rent, services, BESS revenue)
- OPEX modeling with escalation
- Lease-up curve modeling for new facilities

**Outputs**:
- Financial performance metrics
- Required rent levels for target returns
- Scenario comparison tables
- Visualization plots

**Final step**: Complete financial analysis for investment decisions

---

## Running the Workflow

### Sequential Execution
Run notebooks in order (01 → 05) for complete analysis:

```bash
# Step 1: Understand curtailment patterns
jupyter notebook notebooks/01_curtailed_energy_analysis.ipynb

# Step 2: Convert by TAC zones
jupyter notebook notebooks/02_TAC.ipynb

# Step 3: Create LMP vectors
jupyter notebook notebooks/03_lmp_vectors.ipynb

# Step 4: Analyze electricity costs
jupyter notebook notebooks/04_electricity_costs_analysis.ipynb

# Step 5: Financial analysis
jupyter notebook notebooks/05_scenario_financial_analysis.ipynb
```

### Standalone Execution
Each notebook can also be run independently if you have the required input data from previous steps.

## Data Dependencies

```
01_curtailed_energy_analysis.ipynb
    └── Requires: CAISO curtailment Excel files

02_TAC.ipynb
    └── Requires: Output from 01 + TAC zone mappings

03_lmp_vectors.ipynb
    └── Requires: Output from 01 + 02 + LMP data

04_electricity_costs_analysis.ipynb
    └── Requires: LMP data + methodology from previous steps

05_scenario_financial_analysis.ipynb
    └── Requires: Cost analysis from 04 + all previous findings
```

## Key Outputs by Step

1. **Step 1**: Aggregated curtailment data, trend visualizations
2. **Step 2**: TAC zone-mapped data
3. **Step 3**: Representative LMP vectors (CSV files)
4. **Step 4**: Cost comparisons, savings calculations
5. **Step 5**: Financial metrics, minimum viable rent, scenario comparisons

## Tips

- **Start with Step 1**: Always begin with the curtailment analysis to understand the data
- **Check outputs**: Verify each step's outputs before proceeding to the next
- **Data locations**: Note that XLSX files may be in `misc/archived_files/` - update paths as needed
- **Vector files**: The vector CSV files from Step 3 are used in subsequent analyses

## Questions?

Refer to individual notebook documentation for detailed methodology and parameter descriptions.

