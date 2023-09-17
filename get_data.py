import numpy as np
import pickle
from collections import defaultdict
import os

policy = ["FTC_S", "GD"]
mem_cap = [20000, 40000, 60000, 80000, 100000]

data = dict()

for i in policy:
    for j in mem_cap:
        with open("{}-{}-b.pckl}", "r+b") as f:
            tup = pickle.load(f)

            if len(tup) == 3:
                policy, analysis, capacity_misses = tup
            else:
                policy, analysis, capacity_misses, len_trace = tup
            
            if i not in data:
                data[i] = dict()
            if j not in data[i]:
                data[i][j] = dict()

            data[i][j]["overhead"] = analysis["global"]["total_cold"]
            data[i][j]["cold_latency"] = analysis["global"]["total_purecold"]
            data[i][j]["wait_latency"] = analysis["global"]["total_wait"]
            data[i][j]["exec_duration"] = analysis["global"]["total_exec"]
            data[i][j]["cold"] = analysis["global"]["misses"]
            data[i][j]["wait"] = analysis["global"]["hitreuse"]
            data[i][j]["warm"] = analysis["global"]["purehits"]
            

with open("data.pckl", "w+b") as f:
    pickle.dump(data, f)
