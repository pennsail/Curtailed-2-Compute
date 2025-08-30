### Evaluation 

Here is the code for evaluating different datacenter energy solutions while serving a representative workload.

Before diving into specific scenarios, we need a hypothetical baseline to compare against. This baseline represents the current state of the datacenter without any optimizations or workload shifting.

For this purpose, we use Azure VM job traces as our primary dataset. These traces provide detailed information about VM resource usage, including CPU, memory, which are crucial for understanding the baseline performance and energy consumption of the datacenter. The data is pre-processed and saved in `earliest_vm_readings_merged.csv`. 

This csv is too large for GitHub, thus will be shared with drives. 

Run `analyze_week_vms.py` and `plot_week_analysis.py` should be able to provide insights into the energy consumption patterns of VMs over the specified week.