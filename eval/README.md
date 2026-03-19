### Evaluation

To run the full evaluation and generate all comparison plots, run:

```bash
cd eval
python demo_strategies_combined_plot.py
```

This script simulates three datacenter scheduling strategies (as-is, curtailment-only, and carbon-aware) against a representative one-week workload derived from Azure VM traces, and produces plots comparing power usage, carbon emissions, and cost across strategies.
