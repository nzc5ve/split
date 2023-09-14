import os
import pandas as pd
from multiprocessing import Pool
from LambdaData import *
import pickle
from math import ceil
import numpy as np

with open("./1000-b.pckl", "r+b") as f:
    lambdas, trace_old =  pickle.load(f)

second = 1
num_sec = dict()
trace = list()

print(len(trace_old))

for d, t in trace_old:
    if t < 1000 * second: # milisecond
        if d.kind not in num_sec:
            num_sec[d.kind] = [LambdaData(d.kind,d.mem_size,d.run_time,d.warm_time),0]
        num_sec[d.kind][1] += 1
    else:
        k = list(num_sec.keys())
        for i in range(len(k)):
            trace.append((num_sec[k[i]][0], (second-1)*1000+i*1000/len(k), num_sec[k[i]][1]))
        num_sec = dict()
        second += 1

k = list(num_sec.keys())
for i in range(len(k)):
    trace.append((num_sec[k[i]][0], (second-1)*1000+i*1000/len(k), num_sec[k[i]][1]))

print(second, len(trace))

out_trace = sorted(trace, key=lambda x:x[1]) #(lamdata, t)
print(1000, len(out_trace))
with open('870-b.pckl', "w+b") as f:
    data = (lambdas, out_trace)
    pickle.dump(data, f)
