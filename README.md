# Curtailed-2-Compute

**Leveraging curtailed renewable energy for data center computing**

This repository contains the public-facing research workflow for analyzing California ISO (CAISO) curtailment patterns and evaluating parameterized economic scenarios for flexible data center loads. The release is centered on the canonical notebook sequence in `notebooks/01` through `notebooks/05`, supported by lightweight derived datasets and documentation that explain how to reproduce the analysis.

This research was made possible by the generous support of Next10. We are grateful for their financial contributions, as
well as their technical perspective and insights into the California context, which significantly shaped the direction and
depth of this work. Please find our joint report [here](https://www.next10.org/publications/curtail-to-compute).

## Overview

The repository provides:

- Five numbered notebooks (`notebooks/01` through `notebooks/05`) covering CAISO curtailment analysis, LMP vector construction, electricity cost comparison, and financial modeling
- Derived datasets in `data/processed/` for lightweight reproduction
- An `eval/` directory that simulates three data center scheduling strategies (as-is, curtailment-only, carbon-aware) with battery storage and produces comparison plots — run with `cd eval && python demo_strategies_combined_plot.py`

## Quick Start

### Prerequisites

- Python 3.9 or newer
- Jupyter Notebook or JupyterLab
- The packages listed in `requirements.txt`

### Installation

```bash
git clone https://github.com/pennsail/Curtailed-2-Compute.git
cd Curtailed-2-Compute
pip install -r requirements.txt
```

### Data Setup

This repository does **not** require committing raw CAISO downloads. Instead:

1. Read `data/README.md` and `DATA_SOURCES.md`.
2. Download raw CAISO inputs separately when needed.
3. Place raw curtailment workbooks in `data/`.
4. Place monthly LMP files in `data/LMP_Data/`.
5. Use the included derived vectors in `data/processed/` when you want a lightweight starting point.

## Canonical Workflow

Run the notebooks in order because each step depends on outputs or assumptions from earlier stages.

1. `notebooks/01_curtailed_energy_analysis.ipynb`
   Analyzes multi-year CAISO curtailment patterns and creates aggregate statistics.
2. `notebooks/02_TAC.ipynb`
   Maps the curtailment analysis into TAC-zone views for downstream market interpretation.
3. `notebooks/03_lmp_vectors.ipynb`
   Builds representative LMP and curtailment vectors, including the weekly vectors stored in `data/processed/`.
4. `notebooks/04_electricity_costs_analysis.ipynb`
   Compares electricity costs across metro, rural-flexible, and rural-plus-battery scenarios.
5. `notebooks/05_scenario_financial_analysis.ipynb`
   Runs the financial model, minimum viable rent calculations, and sensitivity analysis.

More detailed notebook descriptions live in `WORKFLOW_GUIDE.md` and `NOTEBOOK_INDEX.md`.

## Repository Layout

```text
Curtailed-2-Compute/
├── README.md
├── requirements.txt
├── DATA_SOURCES.md
├── USAGE_GUIDE.md
├── WORKFLOW_GUIDE.md
├── NOTEBOOK_INDEX.md
├── data/
│   ├── README.md
│   ├── processed/
│   │   ├── vector_high_curtailment_week.csv
│   │   └── vector_high_volatility_week.csv
│   └── LMP_Data/                      # Raw monthly LMP downloads kept local
├── notebooks/
│   ├── 01_curtailed_energy_analysis.ipynb
│   ├── 02_TAC.ipynb
│   ├── 03_lmp_vectors.ipynb
│   ├── 04_electricity_costs_analysis.ipynb
│   └── 05_scenario_financial_analysis.ipynb
└── eval/                              # Battery and workload evaluation workflow
```

## Method Summary

The public workflow focuses on three scenario families:

- **Scenario A**: metro baseline with conventional tariff exposure
- **Scenario B**: rural flexible load aligned with curtailment conditions
- **Scenario C**: rural flexible load plus battery storage

Curtailment conditions are identified from CAISO LMP congestion signals, and the financial model evaluates long-run economics through NPV, IRR, MIRR, and minimum viable rent calculations.

## Tests

The `eval/tests/` directory contains a test suite covering the Battery model and DataCenter scheduling strategies:

```bash
python -m pytest eval/tests/ -v
```

## Reproducibility Notes

- Run notebooks `01` to `05` in sequence.
- Review data expectations in `DATA_SOURCES.md` before running from raw inputs.
- Treat files in `data/processed/` as lightweight reproducibility aids, not substitutes for the full raw CAISO archive.
- The `eval/` workflow can be run independently: `cd eval && python demo_strategies_combined_plot.py`.
- For exact dependency pinning, use `pip install -r requirements-lock.txt` instead of `requirements.txt`.

## Documentation

- `README.md`: project overview
- `data/README.md`: included data and external download instructions
- `DATA_SOURCES.md`: source systems, filenames, and field descriptions
- `USAGE_GUIDE.md`: how to run the workflow
- `WORKFLOW_GUIDE.md`: step-by-step description of the notebook sequence
- `NOTEBOOK_INDEX.md`: concise index of the notebooks

## Citation

Please cite this repository using the metadata in `CITATION.cff`.

## License

This project is released under the MIT License. See `LICENSE`.

## Contact

For questions about the public workflow, open an issue in the repository.
