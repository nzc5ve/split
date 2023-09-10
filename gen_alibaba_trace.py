import pandas as pd
import pickle
import argparse
from LambdaData import *
    


def gen_alibaba_trace(file):

    data = pd.read_csv(file)
    overview = dict()
    characteristic = dict()
    lambdas = dict()
    trace = []
    for i in range(len(data['timestamp'])):
        if data['functionName'][i] not in overview:
            overview[data['functionName'][i]] = [0] * (max(data['timestamp']) + 1)
        if data['functionName'][i] not in characteristic:
            characteristic[data['functionName'][i]] = [data['functionName'][i], data['memoryInMB'][i], data['coldstartLatency'][i], data['avgDurationMs'][i]]
        overview[data['functionName'][i]][data['timestamp'][i]] = data['concurrency'][i]

    for func in overview.keys():
        one_func_trace = []
        one_lambda = dict()
        d = LambdaData(characteristic[func][0], characteristic[func][1], characteristic[func][2]+characteristic[func][3], characteristic[func][3])
        one_lambda[characteristic[func][0]] = (characteristic[func][0], characteristic[func][1], characteristic[func][2]+characteristic[func][3], characteristic[func][3])
        for second, requests in enumerate(overview[func]):
            second *= 1000
            if requests == 0:
                continue
            else:
                ivt = 1000 / requests
                one_func_trace += [(d, second+i*ivt) for i in range(requests)]
        lambdas = {**lambdas, **one_lambda}
        trace += sorted(one_func_trace, key=lambda x:x[1])
    alibaba_trace = sorted(trace, key=lambda x:x[1])

    save_pth = "{}-b.pckl".format(len(characteristic))
    with open(save_pth, "w+b") as f:
        #trace = [LambdaData(func_name, mem, e2e, exec_duration), arrival_time]
        data = (lambdas, alibaba_trace)
        pickle.dump(data, f)

    print("done", save_pth)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate Alibaba trace data')
    parser.add_argument("--filename", type=str, default="./trace.csv", required=False)
    args = parser.parse_args()

    file = args.filename
    gen_alibaba_trace(file)

