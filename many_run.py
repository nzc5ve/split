#!/usr/bin/python3
from collections import defaultdict
import multiprocessing as mp
#from TraceGen import *
from LambdaScheduler import LambdaScheduler
import pickle
import argparse
import os
import time
import numpy as np

def load_trace(num_functions, char, trace_path):
    fname = "{}-{}.pckl".format(num_functions, char)
    with open(os.path.join(trace_path, fname), "r+b") as f:
        
        return pickle.load(f)

def compare_pols(policy, num_functions, char, mem_capacity=32000, args=None):
    # save_pth = "/data2/alfuerst/verify-test/"
    # log_dir = "/data2/alfuerst/verify-test/logs/"
    save_pth = args.savedir
    log_dir = args.logdir
    name = "{}-{}-{}-{}.pckl".format(policy, num_functions, mem_capacity, char)
    save_pth = os.path.join(save_pth, name)
    # file = open('/home/qichangl/data/exe_time.txt', 'a')

    #start = time.time()
    if not os.path.exists(save_pth):
        result_dict = dict()
        evdicts = dict()
        misses = dict()
        
        #real_init = dict()

        L = LambdaScheduler(policy, mem_capacity, num_functions, char, log_dir)
        lambdas, trace = load_trace(num_functions, char, args.tracedir)
        #for i in lambdas.keys():
        #    temp = list(lambdas[i])
        #    temp[1] = temp[1] + 150
        #    lambdas[i] = tuple(temp)
        i = 0
        #mempercent = 0
        #file = open('/home/qichangl/data/azure_workload.txt', 'a')
        for d, t in trace:
            #d.run_time = d.warm_time + 2.5*d.mem_size

            #d.mem_size = d.mem_size + 150
            #file.write(str(d.mem_size)+','+str(d.run_time)+','+str(d.warm_time)+"\n")
            i += 1
            L.runActivation(d, t)

            #real_init = L.real_init

            #mempercent += L.runActivation(d, t)
        #file = open('/home/qichangl/data/mem_usage.txt', 'a')
        #file.write(str(mempercent/i)+"_"+name+"\n")
        #file.close()
        #print(mempercent/i)

        if policy == "GD":
            GD_init_wait = "GD_{}_init_wait.npy".format(mem_capacity)
            np.save(GD_init_wait, np.array(L.GD_init_wait))

        
        #L.MemUsageHist.flush()
        L.PerformanceLog.flush()
        #L.PureCacheHist.flush()

        #L.MemUsageHist.close()
        L.PerformanceLog.close()
        #L.PureCacheHist.close()
        

        # policy = string
        # L.evdict:    dict[func_name] = eviction_count
        # L.miss_stats: dict of function names whos entries are  {"misses":count, "hits":count}
        # lambdas:    dict[func_name] = (mem_size, cold_time, warm_time)
        # capacity_misses: dict[func_name] = invocations_not_handled
        # len_trace: long
        
        data = (policy, L.evdict, L.miss_stats(), lambdas, L.capacity_misses, len(trace))
        with open(save_pth, "w+b") as f:
            pickle.dump(data, f)
        

    #end = time.time()
    # file.write(name+"_with time is:"+str(end - start)+"\n")
    # file.close()
    #print("done", name, "and time is:", end-start)
    fname = "{}-{}-{}-{}-performancelog.csv".format(policy, num_functions, mem_capacity, char)

    #if os.path.exists(os.path.join('./data/verify-test/logs/', fname)):
    #    os.remove(os.path.join('./data/verify-test/logs/', fname))

    print("done", name)

def run_multiple_expts(args):
    #policies = ["BMO","BMO_R"]
    #policies = ["MDP", "FTC_R", "FTC_T", "FTC_M", "FTC_S", "GD", "GD_R", "TTL", "LRU", "LRU_R", "LND", "HIST", "FREQ", "SIZE"]
    policies = ["FTC_S","GD","LRU"]
    mems = [20000,30000,40000,50000,60000,70000,80000]#[i for i in range(30000, 40001, args.memstep)]#[i for i in range(520000, 750001, args.memstep)]
    mems = set(mems)
    results = []
    print(len(policies) * len(mems))
    with mp.Pool(40) as pool:
        for policy in policies:
            for mem in mems:
                for num_func in [args.numfuncs]:
                    for char in ["b"]:
                        result = pool.apply_async(compare_pols, [policy, num_func, char, mem, args])
                        results.append(result)
        [result.wait() for result in results]
        # for result in results:
        #     result.wait()
        for result in results:
            r = result.get()
            if r is not None:
                print(r)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run FTC')
    parser.add_argument("--tracedir", type=str, default="./data/trace_pckl/represent/", required=False)
    parser.add_argument("--numfuncs", type=int, default=750, required=False)
    parser.add_argument("--savedir", type=str, default="./data/verify-test/", required=False)
    parser.add_argument("--logdir", type=str, default="./data/verify-test/logs/", required=False)
    parser.add_argument("--memstep", type=int, default=5000, required=False)
    args = parser.parse_args()
    if not os.path.exists(args.savedir):
        os.makedirs(args.savedir)
    if not os.path.exists(args.logdir):
        os.makedirs(args.logdir)

    print(args)
    run_multiple_expts(args)
