# Curtailed-2-Compute

**Leveraging Curtailed Renewable Energy for Datacenter Computing**

This repository provides a comprehensive open-source analysis framework for evaluating opportunities to align flexible computing loads (e.g., data centers) with curtailed renewable energy in California's CAISO grid. The analysis pipeline processes CAISO data to understand curtailment patterns, calculate electricity costs, and perform financial modeling for different deployment scenarios.

## Overview

As California's grid integrates increasing amounts of renewable energy, curtailment—the intentional reduction of renewable generation when supply exceeds demand—has become a significant challenge. This project provides a complete workflow to:

1. **Analyze curtailment patterns** from CAISO data
2. **Calculate electricity costs** for different deployment scenarios
3. **Evaluate financial viability** using NPV, IRR, and other metrics
4. **Compare scenarios** (metro vs. rural, with/without battery storage)

### Key Research Questions

- What are the temporal patterns of renewable energy curtailment in California?
- How can flexible computing loads (e.g., data centers) be aligned with curtailed energy?
- What are the economic implications of utilizing curtailed energy for data center operations?
- How do different deployment scenarios (metro vs. rural, with/without battery storage) compare financially?

## Quick Start

### Prerequisites

- Python 3.8+
- Jupyter Notebook or JupyterLab
- Required packages (see `requirements.txt`)

### Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd Curtailed-2-Compute
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Download CAISO data (see [DATA_SOURCES.md](DATA_SOURCES.md) for details):
   - **Curtailment data**: Available from [CAISO Managing Oversupply](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx)
     - Files: `productionandcurtailmentsdata_YYYY.xlsx`
     - Place in `data/` directory
   - **LMP data**: Available from [CAISO OASIS](http://oasis.caiso.com/)
     - Files: `YYYYMM_LMP.csv` format
     - Place in `data/LMP_Data/` directory

### Running the Analysis Workflow

The analysis follows a sequential workflow from Notebook 01 through Notebook 05. **Run them in order** as each notebook depends on outputs from previous notebooks:

1. **`01_curtailed_energy_analysis.ipynb`** - Analyze curtailment patterns
   - Loads CAISO curtailment Excel files
   - Analyzes temporal patterns (daily, monthly, seasonal)
   - Outputs: Aggregated curtailment data and visualizations

2. **`02_TAC.ipynb`** - TAC zone conversion
   - Maps curtailment data to Transmission Access Charge zones
   - Prepares zone-specific data for downstream analysis

3. **`03_lmp_vectors.ipynb`** - Create representative LMP vectors
   - Combines curtailment and LMP data into 168-hour weekly vectors
   - Identifies representative weeks (high curtailment, high volatility)
   - Optional: Integrates carbon intensity data from WattTime API
   - Outputs: Weekly and monthly representative vectors

4. **`04_electricity_costs_analysis.ipynb`** - Calculate electricity costs
   - Evaluates three deployment scenarios:
     - **Scenario A**: Metro baseline (retrofitted, 67% utilization)
     - **Scenario B**: Rural flexible load (new deployment, 100% utilization, LMP pricing during curtailment)
     - **Scenario C**: Rural with battery storage (BESS + flexible load)
   - Uses PG&E B-20 tariff structure and CAISO LMP data
   - Outputs: Annual electricity costs for each scenario

5. **`05_scenario_financial_analysis.ipynb`** - Financial modeling
   - Calculates NPV, IRR, and MIRR for each scenario
   - Determines minimum viable rent for target returns
   - Performs sensitivity analysis
   - Outputs: Financial metrics and scenario comparisons

## Repository Structure

```
Curtailed-2-Compute/
├── README.md                          # This file: Project overview
├── requirements.txt                   # Python package dependencies
├── notebooks/                         # Main analysis workflow (numbered 01-05)
│   ├── 01_curtailed_energy_analysis.ipynb    # Step 1: Curtailment analysis
│   ├── 02_TAC.ipynb                          # Step 2: TAC zone conversion
│   ├── 03_lmp_vectors.ipynb                  # Step 3: LMP vector creation
│   ├── 04_electricity_costs_analysis.ipynb   # Step 4: Cost scenarios
│   ├── 05_scenario_financial_analysis.ipynb  # Step 5: Financial analysis
│   └── [supporting notebooks]                # Additional analyses
├── data/                              # Data files (not in repo - see DATA_SOURCES.md)
│   ├── LMP_Data/                     # Locational Marginal Price data
│   └── [CAISO datasets]               # See DATA_SOURCES.md for details
├── outputs/                           # Generated analysis outputs
│   └── README.md                     # Output documentation
├── scripts/                           # Standalone Python scripts
├── npv_analysis/                      # Supporting financial analysis
└── misc/                              # Supporting documents and references
```

## Documentation

- **[README.md](README.md)** - This file: Project overview and quick start
- **[WORKFLOW_GUIDE.md](WORKFLOW_GUIDE.md)** - Detailed workflow documentation
- **[NOTEBOOK_INDEX.md](NOTEBOOK_INDEX.md)** - Index of all analysis notebooks
- **[DATA_SOURCES.md](DATA_SOURCES.md)** - Comprehensive data source documentation
- **[USAGE_GUIDE.md](USAGE_GUIDE.md)** - Step-by-step guide for working with CAISO data
- **[outputs/README.md](outputs/README.md)** - Documentation of generated outputs

## Data Sources

### CAISO Curtailment Data
- **Source**: [CAISO Managing Oversupply Reports](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx)
- **Format**: Excel files with Production and Curtailments sheets
- **Temporal Resolution**: 5-minute intervals
- **Coverage**: 2020-2025 (as available)
- **Files**: `productionandcurtailmentsdata_YYYY.xlsx`

### CAISO LMP (Locational Marginal Price) Data
- **Source**: [CAISO OASIS](http://oasis.caiso.com/)
- **Format**: CSV files
- **Temporal Resolution**: Hourly
- **Components**: Energy, Congestion, Losses, GHG components
- **Node**: `TH_NP15_GEN-APND` (Northern California pricing node)
- **Files**: `YYYYMM_LMP.csv` format

### Additional Data Sources
- PG&E B-20 tariff rates (see notebook 04 for source)
- WattTime API for carbon intensity (optional, see notebook 03)
- See [DATA_SOURCES.md](DATA_SOURCES.md) for complete documentation

## Methodology

### Curtailment Identification
Curtailment hours are identified when the congestion component of LMP is negative (`lmp_congestion <= 0`). This indicates periods when renewable energy is being curtailed due to oversupply or transmission constraints.

### Scenario Definitions

**Scenario A: Metro Baseline (Retrofitted, Underutilized)**
- Utilization: 67% of installed capacity (16.08 MW of 24 MW)
- Rationale: Models a retrofitted data center where existing infrastructure is repurposed
- Pricing: Standard PG&E B-20 TOU rates for all hours

**Scenario B: Rural Flexible Load (New Deployment, Full Capacity)**
- Utilization: 100% of installed capacity (24 MW)
- Rationale: Models a new deployment designed for flexible load operations
- Pricing: Market LMP during curtailment hours, TOU rates otherwise

**Scenario C: Rural with Battery Storage**
- Base: Same as Scenario B (100% utilization, flexible load)
- Additions: Battery energy storage system (BESS) for energy arbitrage and ancillary services

### Financial Modeling
- **Time Horizon**: 25 years
- **Discount Rate**: 8% (hurdle rate for NPV)
- **Target IRR**: 15% (levered IRR for equity investors)
- **Robust IRR**: Uses fallback hierarchy (numpy_financial → bisection → MIRR)

## Key Findings

1. **Curtailment Patterns**: Spring months (March-May) show highest curtailment, with peak events exceeding 870 MWh in a single 5-minute interval
2. **Economic Opportunity**: Flexible loads can achieve cost savings by aligning with curtailment hours (Scenario B vs. A: ~$1.9M annual savings)
3. **Battery Storage**: Provides additional revenue through energy arbitrage and ancillary services (Scenario C: ~$0.9M annual revenue)
4. **Financial Viability**: Rural scenarios with flexible loads show improved financial metrics compared to traditional metro deployments

## Reproducibility

This repository is designed for reproducibility:

- **Sequential Workflow**: Notebooks 01-05 must be run in order
- **Data Dependencies**: Each notebook clearly documents required input data
- **Output Tracking**: Intermediate outputs are saved for verification
- **Environment**: `requirements.txt` specifies all dependencies
- **Documentation**: Comprehensive documentation at each step

**Note**: CAISO data files are not included in the repository due to size. Users must download data separately (see [DATA_SOURCES.md](DATA_SOURCES.md)).

## Contributing

This repository is designed to be an open-source guide for working with CAISO data. Contributions are welcome! Please:

1. Follow the existing code structure and documentation style
2. Add clear comments and docstrings to new code
3. Document any new data sources or methodologies
4. Update relevant documentation files
5. Ensure notebooks run sequentially without errors

## Citation

If you use this repository in your research, please cite:

```bibtex
@software{curtailed2compute,
  title = {Curtailed-2-Compute: Leveraging Curtailed Renewable Energy for Datacenter Computing},
  author = {[Your Name]},
  year = {2025},
  url = {[Repository URL]}
}
```

## License

[Specify your license here]

## Contact

For questions or collaboration opportunities, please [specify contact method].

## Acknowledgments

- CAISO for providing publicly available data
- PG&E for tariff rate information
- WattTime for carbon intensity data (optional)

---

**Note**: This repository accompanies a research paper. For detailed methodology and results, please refer to the associated publication and the comprehensive documentation in each notebook.
