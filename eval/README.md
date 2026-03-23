### Evaluation

This directory contains experimental code for evaluating data center energy strategies against a representative workload.

## Public Release Status

This directory is **not part of the canonical open-source reproduction workflow** for the repository at this time.

Reasons:

- the workflow depends on Azure VM job traces that are not public
- some scripts assume local or external data files that are not bundled with the repository
- the battery and workload evaluation path needs a separate review before publication

## Current Data Dependency

The evaluation workflow uses Azure VM job traces as its primary dataset. Those traces describe VM resource usage such as CPU and memory and are intended to represent baseline data center demand.

The referenced pre-processed file is `earliest_vm_readings_merged.csv`, which is not included in the public repository.

## Before Publishing This Directory

Coordinate with Jiali Xing to review:

1. whether the underlying traces can be shared
2. whether synthetic or anonymized substitutes are needed
3. which scripts still rely on hardcoded local paths
4. which outputs are safe to publish as derived artifacts

## Local Usage

If you have approved access to the underlying workload traces, `analyze_week_vms.py` and `plot_week_analysis.py` are the starting points for the local evaluation workflow.