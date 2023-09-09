#!/bin/bash

trace_dir="./data/trace_pckl/represent"
trace_output_dir="./data/verify-test"
log_dir="$trace_output_dir/logs"
memory_dir="$trace_output_dir/memory"
analyzed_dir="$trace_output_dir/analyzed"
plot_dir="./data/figs"


num_funcs=750

memstep=5000

# download trace

# run
python3 ./many_run.py --tracedir $trace_dir --numfuncs $num_funcs --savedir $trace_output_dir --logdir $log_dir --memstep=$memstep

#cd plotting/
#analyze result

#python3 ./split/plotting/compute_mem_usage.py --logdir $log_dir --savedir $memory_dir
python3 ./compute_policy_results.py --pckldir $trace_output_dir --savedir $analyzed_dir

# plot graphs

python3 ./plot_run_across_mem.py --analyzeddir $analyzed_dir --plotdir $plot_dir --numfuncs $num_funcs
python3 ./plot_cold_across_mem.py --analyzeddir $analyzed_dir --plotdir $plot_dir --numfuncs $num_funcs

