# Notebook Index

This document provides an overview of all analysis notebooks in the Curtailed-2-Compute repository.

## Main Analysis Workflow (Numbered Sequence)

The analysis follows a numbered workflow from 01-05:

### 1. `notebooks/01_curtailed_energy_analysis.ipynb`
**Purpose**: Comprehensive analysis of renewable energy curtailment patterns

**Key Features**:
- Loads and combines multiple years of CAISO curtailment data
- Analyzes daily, monthly, and yearly trends
- Visualizes solar vs. wind curtailment contributions
- Identifies seasonal patterns

**Outputs**:
- Aggregated curtailment breakdowns (daily, monthly, yearly)
- Visualization plots
- Statistical summaries

**Data Requirements**:
- CAISO curtailment Excel files in `data/` directory
- Files: `productionandcurtailmentsdata_YYYY.xlsx`

**When to Use**: Start here - this is the first step in the analysis workflow

---

### 2. `notebooks/02_TAC.ipynb`
**Purpose**: TAC zone conversion and analysis

**Key Features**:
- Converts curtailment data by TAC (Transmission Access Charge) zones
- Uses data from CAISO transmission plan report
- Maps curtailment to transmission zones for cost allocation analysis

**When to Use**: Second step - converts data from Notebook 01 for zone-specific analysis

---

### 3. `notebooks/03_lmp_vectors.ipynb`
**Purpose**: LMP vector creation for representative weeks

**Key Features**:
- Creates 168-hour (weekly) vectors with curtailment and LMP data
- Identifies representative weeks (high curtailment, high volatility)
- Optionally adds carbon intensity data from WattTime API
- Generates monthly representative vectors

**Outputs**:
- `vector_high_curtailment_week.csv` - Week with highest curtailment
- `vector_high_volatility_week.csv` - Week with highest LMP volatility
- `monthly_representative_vectors.xlsx` - Monthly representative vectors

**When to Use**: Third step - creates vectors used in subsequent cost and financial analyses

---

### 4. `notebooks/04_electricity_costs_analysis.ipynb`
**Purpose**: Electricity cost analysis for three data center deployment scenarios

**Key Features**:
- Scenario A: Metro baseline (traditional)
- Scenario B: Rural with flexible load
- Scenario C: Rural with battery storage
- Uses CAISO LMP data to identify curtailment hours
- Calculates annual electricity costs for each scenario

**Outputs**:
- Cost comparisons across scenarios
- LMP trend visualizations (duck curve)
- Savings calculations

**Data Requirements**:
- CAISO LMP data files in `data/LMP_Data/` directory
- Files: `YYYYMM_LMP.csv` format

**When to Use**: Fourth step - evaluates economic viability of different deployment scenarios

---

### 5. `notebooks/05_scenario_financial_analysis.ipynb`
**Purpose**: Comprehensive financial analysis (NPV, IRR, minimum viable rent)

**Key Features**:
- Core financial functions: NPV, IRR, MIRR with robust fallback methods
- Model engine: Revenue building, OPEX modeling, lease-up curves
- Scenario configuration: Detailed parameters for three scenarios
- Feasibility analysis: Bankability screening and rent solver
- Calculates Net Present Value (NPV)
- Calculates Internal Rate of Return (IRR) with multiple fallback methods
- Determines minimum viable rent for target IRR (15%)
- Compares three deployment scenarios

**Outputs**:
- Financial metrics for each scenario
- Comparison tables
- Minimum viable rent calculations
- Visualization plots

**When to Use**: Final step - complete financial analysis for investment decision support

---

## Supporting Analysis Notebooks

### `notebooks/curtailment_analysis.ipynb`
**Purpose**: Detailed curtailment analysis (alternative to 01)

**Key Features**:
- Alternative implementation of curtailment analysis
- May include additional analysis methods

**When to Use**: Alternative approach to curtailment analysis

---

### `notebooks/wattime.ipynb`
**Purpose**: Carbon intensity analysis using WattTime API

**Key Features**:
- Fetches carbon intensity data from WattTime API
- Analyzes carbon emissions during curtailment periods
- May integrate with curtailment analysis

**Data Requirements**:
- WattTime API credentials
- CAISO region identifier

**When to Use**: For environmental impact analysis

---

## Supporting Financial Analysis Notebooks

### `npv_analysis/parameter_sweep_analysis.ipynb`
**Purpose**: Comprehensive financial analysis (NPV, IRR, payback period, minimum viable rent)

**Key Features**:
- Core financial functions: NPV, IRR, MIRR with robust fallback methods
- Model engine: Revenue building, OPEX modeling, lease-up curves
- Scenario configuration: Detailed parameters for three scenarios
- Feasibility analysis: Bankability screening and rent solver
- Calculates Net Present Value (NPV)
- Calculates Internal Rate of Return (IRR) with multiple fallback methods
- Determines minimum viable rent for target IRR (15%)
- Compares three deployment scenarios

**Outputs**:
- Financial metrics for each scenario
- Comparison tables
- Minimum viable rent calculations
- Visualization plots

**When to Use**: For understanding model sensitivity to key parameters

---

### `npv_analysis/revenue_target_analysis.ipynb`
**Purpose**: Parameter sensitivity analysis

**Key Features**:
- Sweeps key parameters (amortization period, scale, utilization, revenue growth, discount rate)
- Sensitivity analysis
- Optimization insights

**When to Use**: For determining revenue requirements

---

## Root-Level Notebooks

### 11. `parameter_sweep_analysis.ipynb`
**Purpose**: Parameter sensitivity analysis

**Key Features**:
- Sweeps key parameters (amortization period, scale, utilization, etc.)
- Sensitivity analysis
- Optimization insights

**When to Use**: For understanding model sensitivity to key parameters

---

### 12. `revenue_target_analysis.ipynb`
**Purpose**: Revenue target analysis

**Key Features**:
- Calculates required revenue for target IRR
- Revenue gap analysis

**When to Use**: For determining revenue requirements

---

### 13. `datacenter_electricity_cost_analysis.ipynb`
**Purpose**: Data center electricity cost analysis (may be duplicate or alternative version)

**Key Features**:
- Similar to `notebooks/electricity_costs_analysis.ipynb`
- May have different implementation

**When to Use**: Alternative approach to cost analysis

---

## Recommended Workflow

### For New Users

Follow the numbered workflow sequence:

1. **Start with**: `notebooks/01_curtailed_energy_analysis.ipynb`
   - Understand curtailment patterns
   - Get familiar with data structure

2. **Then**: `notebooks/02_TAC.ipynb`
   - Convert data by TAC zones
   - Understand zone-specific patterns

3. **Next**: `notebooks/03_lmp_vectors.ipynb`
   - Create representative LMP vectors
   - Identify key weeks for analysis

4. **Then**: `notebooks/04_electricity_costs_analysis.ipynb`
   - Understand cost implications
   - See how curtailment affects costs

5. **Finally**: `notebooks/05_scenario_financial_analysis.ipynb`
   - Complete financial analysis
   - Investment decision support

### For Advanced Analysis

1. **Parameter Sensitivity**: `parameter_sweep_analysis.ipynb`
2. **Carbon Impact**: `notebooks/wattime.ipynb`
3. **Integrated Analysis**: `notebooks/curtailment_lmp_vectors.ipynb`

## Notebook Dependencies

```
01_curtailed_energy_analysis.ipynb
    └── (no dependencies)

electricity_costs_analysis.ipynb
    └── Requires: LMP data files

curtailment_lmp_vectors.ipynb
    └── Requires: Curtailment data + LMP data + WattTime API

01_curtailed_energy_analysis.ipynb
    └── (no dependencies - first step)

02_TAC.ipynb
    └── Uses: Results from 01_curtailed_energy_analysis.ipynb

03_lmp_vectors.ipynb
    └── Uses: Results from 01_curtailed_energy_analysis.ipynb and 02_TAC.ipynb

04_electricity_costs_analysis.ipynb
    └── Uses: LMP data and methodology from previous notebooks

05_scenario_financial_analysis.ipynb
    └── Uses: Results from 04_electricity_costs_analysis.ipynb and all previous analyses
```

## Data File Locations

- **Curtailment Data**: `data/productionandcurtailmentsdata_YYYY.xlsx`
- **LMP Data**: `data/LMP_Data/YYYYMM_LMP.csv`
- **Outputs**: `outputs/` directory
- **Processed Data**: Various locations (check individual notebooks)

## Notes

- Some notebooks may have overlapping functionality
- Check notebook headers for specific data requirements
- Always verify data file paths before running notebooks
- See [DATA_SOURCES.md](DATA_SOURCES.md) for detailed data documentation
- See [USAGE_GUIDE.md](USAGE_GUIDE.md) for step-by-step instructions

