# Notebook Index

## Notebooks

### `notebooks/01_curtailed_energy_analysis.ipynb`

- starting point for the workflow
- analyzes raw CAISO curtailment workbooks
- generates core curtailment summaries used downstream

### `notebooks/02_TAC.ipynb`

- converts the curtailment analysis into TAC-zone views
- supports the geographic interpretation used later in the workflow

### `notebooks/03_lmp_vectors.ipynb`

- merges curtailment context with LMP data
- identifies representative weeks
- produces the derived vectors stored in `data/processed/`

### `notebooks/04_electricity_costs_analysis.ipynb`

- compares electricity cost outcomes across the main deployment scenarios
- uses the representative vectors and tariff assumptions from the earlier notebooks

### `notebooks/05_scenario_financial_analysis.ipynb`

- runs the parameterized economic model
- computes NPV, IRR, MIRR, minimum viable rent, and sensitivity outputs

## Supporting Script

### `notebooks/build_avg_daily_curtailment_by_season_and_year.py`

- helper script for producing one of the summary CSV artifacts from the notebook workflow

## Recommended Order

Run the notebooks in this order:

1. `notebooks/01_curtailed_energy_analysis.ipynb`
2. `notebooks/02_TAC.ipynb`
3. `notebooks/03_lmp_vectors.ipynb`
4. `notebooks/04_electricity_costs_analysis.ipynb`
5. `notebooks/05_scenario_financial_analysis.ipynb`

See `WORKFLOW_GUIDE.md` for the narrative description and `USAGE_GUIDE.md` for setup and data instructions.

