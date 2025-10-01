# Data Center NPV Analysis

This folder contains financial analysis notebooks for the Curtailed-2-Compute data center project.

## Files

### `datacenter_NPV_analysis.ipynb`
Comprehensive financial analysis comparing three data center scenarios:

**Scenarios Analyzed:**
- **Scenario A**: Metro Baseline (No flexibility, no battery)
- **Scenario B**: Rural Flexible Load (Load shifting capabilities)  
- **Scenario C**: Rural with Battery Storage (BESS + load flexibility)

**Key Features:**
- NPV, IRR, and TCO calculations for each scenario
- Revenue gap analysis showing what's needed for 10% IRR target
- Enhanced scenario comparison with original vs. target performance

**Outputs:**
- Current financial performance with given revenue assumptions
- Required revenue levels to achieve 10% IRR
- Revenue gaps and percentage increases needed
- Summary tables comparing all scenarios

**Usage:**
Run all cells to perform complete financial analysis. The notebook uses parameters from the project spreadsheet and provides both current viability assessment and target revenue requirements.

## Key Insights
- All scenarios currently show negative NPVs with base revenue assumptions
- Significant revenue increases (40-60%) needed to achieve 10% IRR
- Rural scenarios (B & C) require smaller absolute revenue increases
- Battery storage (Scenario C) provides modest additional revenue potential