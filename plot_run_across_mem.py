#!/usr/bin/python3
from collections import defaultdict
import numpy as np
import multiprocessing as mp
import pickle
from collections import defaultdict
import os
from pprint import pprint

import matplotlib as mpl
mpl.rcParams.update({'font.size': 14})
mpl.use('Agg')
import matplotlib.pyplot as plt
import argparse

data_path = "./data/verify-test/analyzed/"
plot_dir = "./data/figs/"

def plot_results(result_dict, save_path):
    fig, ax = plt.subplots()
    plt.tight_layout()
    fig.set_size_inches(5,3)
    pols = ["FTC_S", "GD", "LRU"]
    colors = ["black", "b", "tab:red", "tab:green", "tab:pink", "darkorange", "tab:purple", "tab:brown", "c", "tab:gray"]
    markers = ["o", "^", "1", "p", "*", "+", "x", "D", "h"]
    style = ["-", ":", "dashdot"]
    
    for i, policy in enumerate(pols):
        pts = sorted(result_dict[policy], key=lambda x: x[0])
        xs = [x/1024 for x,y in pts]
        if policy in ["FTC_S", "GD", "LRU"]:

            ys = [y*100 for x,y in pts]
            ax.plot(xs, ys, label=policy, linestyle=style[i%3], color=colors[i])
        #ys = [y*100 for x,y in pts]
        #print(ys)
        print(policy, colors[i])

    ax.set_ylabel("Increase in\nexecution delay (%)", fontsize=19)
    ax.set_xlabel("Memory capacity (GB)", fontsize=19)
    ax.tick_params(labelsize=18)
    #ax.yaxis.set_ticks([0,20,40,60,80,100])
    ax.yaxis.set_ticks([0,2,4,6,8,10])
    ax.legend(bbox_to_anchor=(1.025,.68), loc="right", columnspacing=0.5, fontsize=15, ncol=2)
    #ax.legend(bbox_to_anchor=(1.025,.60), loc="right", columnspacing=0.5, fontsize=15, ncol=1)
    print(save_path)
    #ax.set_ylim(0,40)
    plt.savefig(save_path, bbox_inches="tight")
    plt.close(fig)


def average_dicts(dlist):
    n = len(dlist)
    outdict = dict()
    #Assume that the keys are the same for all the dictionaries. 
    #Simple dicts only, not nested or anything
    keys = dlist[0].keys()
    for k in keys:
        vals = np.mean([adict[k] for adict in dlist])
        outdict[k] = vals 
        
    return outdict

def get_info_from_file(filename):
    if "LONG-TTL" in filename:
        num_funcs, mem, run = filename[:-5][9:].split("-")
        policy = "LONG-TTL"
    else:
        policy, num_funcs, mem, run = filename[:-5].split("-")
    return policy, int(num_funcs), int(mem), run

def load_data(path):
    with open(path, "r+b") as f:
        return pickle.load(f)

def plot_run(results_dict, num_funcs):
    results_per_policy = defaultdict(list) # p -> [global-dicts]

    for mem in results_dict.keys():
        # if mem <= 50000:
            for policy in results_dict[mem].keys():
                analysis = results_dict[mem][policy]
                results_per_policy[policy].append((mem , analysis["global"]["total_cold"]/(analysis["global"]["total_cold"]+analysis["global"]["total_exec"])))
                
                print(results_per_policy)

    pth = os.path.join(plot_dir, "exec_time-{}.pdf".format(num_funcs))
    plot_results(results_per_policy, pth)

def plot_all(args):
    data = dict()
    funcs = args.numfuncs
    filt = "-{}-".format(funcs)
    for file in os.listdir(data_path):
        if filt in file and "b" in file and "LONG-TTL" not in file:
            policy, num_funcs, mem, run = get_info_from_file(file)
            if 5000 <= mem <= 105000:
                # policy: string
                # analysis: output from analyze_timings
                # capacity_misses: dict[func_name] = invocations_not_handled
                # len_trace: long
                # print(file)
                tup = load_data(os.path.join(data_path, file))
                # print(tup)
                if len(tup) == 3:
                    policy, analysis, capacity_misses = tup
                else:
                    policy, analysis, capacity_misses, len_trace = tup
                if mem not in data:
                    data[mem] = dict()

                data[mem][policy] = analysis


    plot_run(data, funcs)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='plot FaasCache Simulation')
    parser.add_argument("--analyzeddir", type=str, default="./data/verify-test/analyzed/", required=False)
    parser.add_argument("--plotdir", type=str, default="./data/figs", required=False)
    parser.add_argument("--numfuncs", type=int, default=325, required=False)
    args = parser.parse_args()
    data_path = args.analyzeddir
    plot_dir = args.plotdir
    plot_all(args)
