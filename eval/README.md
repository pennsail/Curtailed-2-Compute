### Evaluation

This directory simulates data center scheduling strategies against a representative one-week workload and compares power usage, carbon emissions, and cost outcomes.

#### Quick Start

```bash
cd eval
python demo_strategies_combined_plot.py
```

This runs the primary evaluation and generates comparison plots for three strategies (as-is, curtailment-only, and carbon-aware), both with and without battery storage.

#### Files

**Core modules:**

| File | Description |
| --- | --- |
| `datacenter.py` | `DataCenter` class implementing three scheduling strategies: `as_is`, `carbon_aware`, and `only_curtail` |
| `battery.py` | `Battery` class modeling charge/discharge cycles for storage-augmented scheduling |

**Main demo scripts (standalone):**

| File | Description |
| --- | --- |
| `demo_strategies_combined_plot.py` | Runs both no-battery and battery scenarios and saves a combined comparison plot |
| `demo_strategies_plot.py` | Runs the three strategies without battery and plots results |
| `demo_strategies_battery_plot.py` | Runs the three strategies with battery and plots results |

**Analysis scripts (may require intermediate outputs):**

| File | Description |
| --- | --- |
| `battery_analysis.py` | Sweeps battery parameters across multiple configurations using `ProcessPoolExecutor` |
| `battery_impact_analysis.py` | Analyzes battery impact on scheduling metrics from sweep results |
| `curtail_only_analysis.py` | Detailed analysis of the curtailment-only strategy outcomes |

**Data preprocessing (requires external data):**

| File | Description |
| --- | --- |
| `analyze_azure_vms.py` | Processes raw Azure VM trace data into summary statistics |
| `analyze_week_vms.py` | Analyzes merged VM readings for weekly patterns |

**Plotting utilities:**

| File | Description |
| --- | --- |
| `plot_data.py` | Plots curtailment and volatility vector data |
| `plot_time_series.py` | Time-series visualization of grid vectors |
| `plot_week_analysis.py` | Plots weekly VM workload analysis results |
| `plot_battery_impact_from_csv.py` | Visualizes battery impact analysis from CSV output |

#### Data Files

| File | Description |
| --- | --- |
| `vmtable.csv` | Synthetic Azure VM workload (bundled for reproducibility) |
| `vector_high_curtailment_week_v2.csv` | Representative 168-hour high-curtailment week |
| `vector_high_volatility_week_v2.csv` | Representative 168-hour high-volatility week |

These vector files are derived from the same analysis in `notebooks/03_lmp_vectors.ipynb` (stored as `data/processed/vector_*.csv` in the main repo).

#### Dependencies

All dependencies are listed in the top-level `requirements.txt`. The eval scripts additionally require `tqdm` for progress bars.
