# Usage Guide

This guide explains how to use the public `Curtailed-2-Compute` workflow without relying on private or oversized local artifacts.

## 1. Set Up the Environment

Install the Python environment from the repository root:

```bash
pip install -r requirements.txt
```

You will need Jupyter Notebook or JupyterLab to run the workflow.

## 2. Prepare the Data

There are two supported ways to work with the repository.

### Lightweight path

Use the included derived vectors in `data/processed/` to inspect the later-stage workflow and sanity-check the economics notebooks without first downloading the full raw archive.

### Full reproduction path

Download the external CAISO files described in `DATA_SOURCES.md`:

- raw curtailment workbooks to `data/`
- monthly LMP CSV files to `data/LMP_Data/`

Keep those raw downloads local. They do not need to be committed to GitHub.

### WattTime API (optional, notebook 03 only)

Notebook 03 fetches marginal carbon intensity data from the [WattTime API](https://watttime.org/) to build the carbon column in the representative weekly vectors. To use this:

1. Register for a free account at [WattTime](https://watttime.org/get-the-data/data-plans/).
2. Set your credentials as environment variables before running the notebook:
   ```bash
   export WATTTIME_USER="your_username"
   export WATTTIME_PASSWORD="your_password"
   ```

If you do not have WattTime credentials, you can still use the **lightweight path** — the pre-built vectors in `data/processed/` and the `eval/` directory already include carbon intensity data, so this step is only needed for full end-to-end reproduction from raw data.

## 3. Run the Canonical Notebook Sequence

Run the notebooks in order:

1. `notebooks/01_curtailed_energy_analysis.ipynb`
2. `notebooks/02_TAC.ipynb`
3. `notebooks/03_lmp_vectors.ipynb`
4. `notebooks/04_electricity_costs_analysis.ipynb`
5. `notebooks/05_scenario_financial_analysis.ipynb`

The workflow is sequential. Earlier notebooks define cleaned inputs and scenario assumptions that are consumed by later steps.

## 4. What Each Notebook Needs

| Notebook | Primary inputs | Primary outputs |
| --- | --- | --- |
| `01_curtailed_energy_analysis.ipynb` | Raw CAISO curtailment workbooks | Daily, monthly, and yearly curtailment summaries |
| `02_TAC.ipynb` | Notebook 01 outputs and TAC mapping inputs | Zone-level curtailment views |
| `03_lmp_vectors.ipynb` | Curtailment results plus monthly LMP files | Representative weekly vectors in `data/processed/` |
| `04_electricity_costs_analysis.ipynb` | LMP vectors and tariff assumptions | Scenario electricity cost comparisons |
| `05_scenario_financial_analysis.ipynb` | Scenario cost outputs and model assumptions | NPV, IRR, rent, and sensitivity results |

## 5. Troubleshooting

### Missing raw files

- Confirm that `productionandcurtailmentsdata_YYYY.xlsx` files are in `data/`.
- Confirm that `YYYYMM_LMP.csv` files are in `data/LMP_Data/`.

### Timezone inconsistencies

- LMP exports may arrive in GMT.
- Normalize timestamps before comparing them with curtailment-derived series.

### Large-memory runs

- Load one year or one month at a time when exploring raw data.
- Save compact intermediate results instead of repeatedly re-reading the full raw archive.

### Evaluation workflow

- The `eval/` directory includes a bundled synthetic workload and runs out of the box.
- See `eval/README.md` for details.

## 6. Related Docs

- `README.md` for project overview
- `DATA_SOURCES.md` for source systems and field descriptions
- `WORKFLOW_GUIDE.md` for the notebook-by-notebook narrative
- `NOTEBOOK_INDEX.md` for a quick reference to the notebooks

