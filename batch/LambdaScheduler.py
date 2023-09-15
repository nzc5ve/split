import pandas as pd
import numpy as np
import scipy.stats as stats
from math import floor, isnan
import random
import heapq
from collections import defaultdict
from LambdaData import *
from Container import *
#from TraceGen import *
from time import sleep
import os
import time
import pickle

class LambdaScheduler:

    hist_num_cols = [i for i in range(4*60)]

    def __init__(self, policy:str="GD", mem_capacity:int=32000, num_funcs:int=10, run:str="a", log_dir=""):
        # log_dir = "/data2/alfuerst/azure/functions/trace_pckl/middle/logs/"
        fname = "{}-{}-{}-{}-".format(policy, num_funcs, mem_capacity, run)

        self.mem_capacity = mem_capacity
        self.mem_used = 0
        self.eviction_policy = policy
        self.Clock = 0
        self.wall_time = 0
        self.finish_times = []
        self.running_c = dict()
        self.ContainerPool = []
        '''
        self.gantt = []
        self.shady = dict()
        self.boxplot = []
        '''
        #with open("./data/trace_pckl/represent/BMO_trace.pckl", "r+b") as f:
        #    self.BMO_trace = pickle.load(f)
        self.BMO_trace = 1

        self.real_init = dict()

        self.PerfLogFName = os.path.join(log_dir, fname+"performancelog.csv")
        self.PerformanceLog = open(self.PerfLogFName, "w")
        self.PerformanceLog.write("lambda,coldtime,exectime,meta\n")

        #self.MemUsageFname = os.path.join(log_dir, fname+"memusagehist.csv")
        #self.MemUsageHist = open(self.MemUsageFname, "w")
        #self.MemUsageHist.write("time,reason,mem_used,mem_size,extra\n")

        #self.PureCacheFname = os.path.join(log_dir, fname+"purecachehist.csv")
        #self.PureCacheHist = open(self.PureCacheFname, "w")
        #self.PureCacheHist.write("time,used_mem,running_mem,pure_cache\n")

        self.evdict = defaultdict(int)
        self.capacity_misses = defaultdict(int)
        self.TTL = 10 * 60 * 1000  # 10 minutes in ms
        self.Long_TTL = 2 * 60 * 60 * 1000  # 2 hours in ms

        self.IT_histogram = dict()
        self.last_seen = dict() # func-name : last seen time
        self.wellford = dict() # func-name : aggregate
        self.histTTL = dict() # func-name : time to live
        self.histPrewarm = dict() # func-name : prewarm time
        self.rep = dict() # func-name : LambdaData; used to prewarm containers

        heapq.heapify(self.ContainerPool)

    ##############################################################

    #def WriteMemLog(self, reason, wall_time, mem_used, mem_size, extra="N/A"):
    #    msg = "{},{},{},{},{}\n".format(wall_time, reason, mem_used, mem_size, str(extra))
    #    self.MemUsageHist.write(msg)

    ##############################################################

    #def WritePerfLog(self, d:LambdaData, time, meta):
    #    msg = "{},{},{}\n".format(d.kind, time, meta)
    #    self.PerformanceLog.write(msg)

    def WritePerfLog(self, d:LambdaData, coldtime, exectime, meta):
        msg = "{},{},{},{}\n".format(d.kind, coldtime, exectime, meta)
        self.PerformanceLog.write(msg)

    ##############################################################

    #def WritePureCacheHist(self, time):
    #    # time, used_mem, running_mem, pure_cache
    #    running_mem = sum([k.metadata.mem_size for k in self.running_c.keys()])
    #    pure_cache = self.mem_used - running_mem
    #    if pure_cache < 0:
    #      raise Exception("Impossible pure_cache allocation: {}".format(pure_cache))
    #    msg = "{},{},{},{}\n".format(time, self.mem_used, running_mem, pure_cache)
    #    self.PureCacheHist.write(msg)


    ##############################################################

    # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
    # For a new value newValue, compute the new count, new mean, the new M2.
    # mean accumulates the mean of the entire dataset
    # M2 aggregates the squared distance from the mean
    # count aggregates the number of samples seen so far
    def well_update(self, existingAggregate, newValue):
        (count, mean, M2) = existingAggregate
        count += 1
        delta = newValue - mean
        mean += delta / count
        delta2 = newValue - mean
        M2 += delta * delta2

        return (count, mean, M2)

    # Retrieve the mean, variance and sample variance from an aggregate
    def well_finalize(self, existingAggregate):
        (count, mean, M2) = existingAggregate
        if count < 2:
            return float('nan'), float('nan'), float('nan')
        else:
            (mean, variance, sampleVariance) = (mean, M2 / count, M2 / (count - 1))
        return (mean, variance, sampleVariance)

    ##############################################################

    def _find_precentile(self, cdf, percent, head=False):
        """ Returns the last whole bucket (minute) before the percentile """
        for i, value in enumerate(cdf):
            if percent < value:
                if head:
                    return max(0, i-1)
                else:
                    return min(i+1, len(cdf))
        return len(cdf)

    ##############################################################

    def PurgeOldHist(self, container_list):
        """ Return list of still usable containers after purging those older than TTL """
        kill_list = [c for c in container_list
                     if c.last_access_t + self.histTTL[c.metadata.kind] < self.wall_time]

        for k in kill_list:
            self.RemoveFromPool(k, "HIST-TTL-purge")
            kind = k.metadata.kind
            self.evdict[kind] += 1

        #This is just the inverse of kill_list. Crummy way to do this, but YOLO
        valid_containers = [c for c in container_list
                     if c.last_access_t + self.histTTL[c.metadata.kind] >= self.wall_time]

        return valid_containers

    ##############################################################


    ##############################################################

    def find_container(self, d: LambdaData):
        """ search through the containerpool for matching container """
        if len(self.ContainerPool) == 0 :
            return [], []
        containers_for_the_lambda = [x for x in self.ContainerPool if (x.metadata == d and
                                                     x not in self.running_c)]
        
        #find warm containers to reuse
        if (self.eviction_policy in ["MDP","FTC_T","FTC_S","FTC_M","GD_R","LRU_R","FTC_R","BMO_R"]) and (containers_for_the_lambda == []):
            all_warm_containers = [x for x in self.ContainerPool if (x.metadata == d and
                                                     x in self.running_c)]
            warm_containers_to_reuse = sorted(all_warm_containers, key=lambda x:self.running_c[x][1])
        else:
            warm_containers_to_reuse = []

        return containers_for_the_lambda, warm_containers_to_reuse
        #Just return the first element.
        #Later on, maybe sort by something? Priority? TTL ?

    ##############################################################

    def pool_stats(self):
        pool = self.ContainerPool #Is a heap
        sdict = defaultdict(int)
        for c in pool:
            k = c.metadata.kind
            sdict[k] += 1

        return sdict

    ##############################################################

    def container_clones(self, c):
        return [x for x in self.ContainerPool if x.metadata == c.metadata]

    ##############################################################

    def calc_priority(self, c, t, BMO_trace, update=False):
        """ GD prio calculation as per eq 1 .
        If update==True, then dont replace the clock value. 
        Modifies insert_clock container state. 
        """

        #It makes sense to have per-container clock instead of per-function.
        #The "oldest" container of the function will be evicted first

        if not update:
            clock = self.Clock
        else:
            clock = c.insert_clock

        prio = c.last_access_t
        
        if self.eviction_policy == "BMO":
            #next_access = BMO_trace[c.metadata.kind][self.wall_time]
            #prio = -next_access
            next_access = 1

        elif self.eviction_policy == "BMO_R":
            #next_access = BMO_trace[c.metadata.kind][self.wall_time]
            #prio = -next_access
            next_access = 1

        elif self.eviction_policy == "MDP":
            freq = c.frequency
            #freq is the frequency of the current container
            cost = float(c.metadata.run_time - c.metadata.warm_time)  # cost is the cold start time
            size = c.metadata.mem_size
            rank = max(len(self.container_clones(c)), 1) # order of the node in the tree (c.node_order)
            prio = clock + freq*(cost/size)/rank

        elif self.eviction_policy == "FTC_T":
            freq = sum([x.frequency for x in self.container_clones(c)])
            #freq is the frequency of the current container
            cost = float(c.metadata.run_time - c.metadata.warm_time)  # cost is the cold start time
            size = c.metadata.mem_size
            rank = max(len(self.container_clones(c)), 1) # order of the node in the tree (c.node_order)
            prio = clock + freq*(cost/size)/rank
            
        elif self.eviction_policy == "FTC_M":
            freq = c.frequency/(t+1)
            #freq is the frequency of the current container
            cost = float(c.metadata.run_time - c.metadata.warm_time)  # cost is the cold start time
            size = c.metadata.mem_size
            rank = max(len(self.container_clones(c)), 1) # order of the node in the tree (c.node_order)
            prio = clock + freq*(cost/size)/rank
            
        elif self.eviction_policy == "FTC_S":
            freq = sum([x.frequency for x in self.container_clones(c)])/(t//1000+1)
            #freq is the frequency of the current container
            cost = float(c.metadata.run_time - c.metadata.warm_time)  # cost is the cold start time
            size = c.metadata.mem_size
            rank = max(len(self.container_clones(c)), 1) # order of the node in the tree (c.node_order)
            prio = clock + freq*(cost/size)/rank

        elif self.eviction_policy == "FTC":
            freq = sum([x.frequency for x in self.container_clones(c)])/(t//(1000*60)+1)
            #freq is the frequency of the current container
            cost = float(c.metadata.run_time - c.metadata.warm_time)  # cost is the cold start time
            size = c.metadata.mem_size
            rank = max(len(self.container_clones(c)), 1) # order of the node in the tree (c.node_order)
            prio = clock + freq*(cost/size)/rank

        elif self.eviction_policy == "GD":
            freq = sum([x.frequency for x in self.container_clones(c)])
            #freq shoud be of all containers for this lambda actiion, not just this one...
            cost = float(c.metadata.run_time - c.metadata.warm_time)  # run_time - warm_time, or just warm_time , or warm/run_time
            size = c.metadata.mem_size
            prio = clock + freq*(cost/size)

        elif self.eviction_policy == "GD_R":
            freq = sum([x.frequency for x in self.container_clones(c)])
            #freq shoud be of all containers for this lambda actiion, not just this one...
            cost = float(c.metadata.run_time - c.metadata.warm_time)  # run_time - warm_time, or just warm_time , or warm/run_time
            size = c.metadata.mem_size
            prio = clock + freq*(cost/size)

        elif self.eviction_policy == "LND":
            # For now, assume this is called only on accesses.
            cost = c.metadata.warm_time # can also be R - W  time
            prio = cost

        elif self.eviction_policy == "FREQ":
            freq = sum([x.frequency for x in self.container_clones(c)])
            cost = float(c.metadata.run_time - c.metadata.warm_time)  # run_time - warm_time, or just warm_time , or warm/run_time
            prio = clock + freq*cost
        elif self.eviction_policy == "SIZE":
            freq = sum([x.frequency for x in self.container_clones(c)])
            size = c.metadata.mem_size
            prio = clock + freq/size
        elif self.eviction_policy == "RAND":
            prio = np.random.randint(10)
        elif self.eviction_policy == "LRU":
            prio = c.last_access_t

        elif self.eviction_policy == "LRU_R":
            prio = c.last_access_t

        elif self.eviction_policy == "TTL":
            prio = c.last_access_t
        elif self.eviction_policy == "LONG-TTL":
            prio = c.last_access_t
        elif self.eviction_policy == "HIST":
            prio = c.last_access_t

        return prio

    ##############################################################

    def checkfree(self, d, n):
        mem_size = n * d.mem_size
        return mem_size + self.mem_used <= self.mem_capacity

    ##############################################################

    def AddToPool(self, d: LambdaData, t, n, parallel=False, prewarm:bool=False, at_time=None):
        if not prewarm and at_time is not None:
            raise Exception("Can only add container at self.wall_time when not prewarming")

        mem_size = d.mem_size
        num_cold = 0

        while num_cold < n:
            if mem_size + self.mem_used <= self.mem_capacity:
                self.mem_used = self.mem_used + mem_size
                c = Container(d)
                c.last_access_t = self.wall_time
                c.keep_alive_start_t = self.wall_time
                c.Priority = self.calc_priority(c,self.wall_time,self.BMO_trace)
                c.insert_clock = self.Clock #Need this when recomputing priority
                num_cold += 1
                heapq.heappush(self.ContainerPool, c)

                c.run()
                #Need to update priority here?
                if parallel == False:
                    processing_time = d.run_time
                    self.running_c[c] = (t, t+processing_time)
                    self.WritePerfLog(d, d.run_time-d.warm_time, d.warm_time, "miss")
                else:
                    processing_time = d.run_time - d.warm_time
                    self.running_c[c] = (t, t+processing_time)

            else:
                for i in range(n-num_cold):
                    self.WritePerfLog(d, d.run_time-d.warm_time, d.warm_time, "miss")
                break
        return num_cold, n-num_cold


    ##############################################################

    def RemoveFromPool(self, c: Container, reason: str):
        if c in self.running_c:
            raise Exception("Cannot remove a running container")
        self.ContainerPool.remove(c)
        self.mem_used -= c.metadata.mem_size

        #self.WriteMemLog(reason, self.wall_time, self.mem_used, c.metadata.mem_size)
        heapq.heapify(self.ContainerPool)

    ##############################################################

    def PurgeOldLongTTL(self, container_list):
        """ Return list of still usable containers after purging those older than TTL """

        kill_list = [c for c in container_list
                     if c.last_access_t + self.Long_TTL < self.wall_time]

        #Aargh this is duplicated from Eviction. Hard to merge though.
        for k in kill_list:
            self.RemoveFromPool(k, "TTL-purge")
            kind = k.metadata.kind
            self.evdict[kind] += 1

        #This is just the inverse of kill_list. Crummy way to do this, but YOLO
        valid_containers = [c for c in container_list
                     if c.last_access_t + self.Long_TTL >= self.wall_time]

        heapq.heapify(self.ContainerPool)
        return valid_containers

    ##############################################################

    def PurgeOldTTL(self, container_list):
        """ Return list of still usable containers after purging those older than TTL """

        kill_list = [c for c in container_list
                     if c.last_access_t + self.TTL < self.wall_time]

        #Aargh this is duplicated from Eviction. Hard to merge though.
        for k in kill_list:
            self.RemoveFromPool(k, "TTL-purge")
            kind = k.metadata.kind
            self.evdict[kind] += 1

        #This is just the inverse of kill_list. Crummy way to do this, but YOLO
        valid_containers = [c for c in container_list
                     if c.last_access_t + self.TTL >= self.wall_time]

        heapq.heapify(self.ContainerPool)
        return valid_containers

    ##############################################################


    def Landlord_Charge_Rent(self):
        """ Return a list of containers to be evicted """
        #Go over all containers, charging rent
        #Then evict lowest credit containers...
        deltas = [float(c.Priority)/c.metadata.mem_size for c in self.ContainerPool]
        delta = min(deltas)
        for c in self.ContainerPool:
            c.Priority = c.Priority - (delta*c.metadata.mem_size)

        heapq.heapify(self.ContainerPool)

    ############################################################

    def Eviction_Priority_Based(self, to_free, eviction_target):
        """ Return save and victim lists for Priority based methods """
        save = []
        eviction_list = []

        while to_free > eviction_target and len(self.ContainerPool) > 0:
            # XXX Can't evict running containers right?
            # Even with infinite concurrency, container will still exist in running_c
            # cleanup_finished
            heapq.heapify(self.ContainerPool)
            victim = heapq.heappop(self.ContainerPool)
            if victim in self.running_c:
                save.append(victim)
            else:
                eviction_list.append(victim)
                to_free -= victim.metadata.mem_size

        return save, eviction_list

    #############################################################

    def Eviction(self, d: LambdaData, n):
        """ Return a list of containers to be evicted """
        to_free = n * d.mem_size

        eviction_target = 0

        if len(self.running_c) == len(self.ContainerPool):
            # all containers busy
            return []

        save, eviction_list = self.Eviction_Priority_Based(to_free, eviction_target)

        for v in eviction_list:
            self.mem_used -= v.metadata.mem_size
            k = v.metadata.kind
            self.evdict[k] += 1

        for c in save:
            heapq.heappush(self.ContainerPool, c)

        #Supposed to set clock = max(eviction_list)
        #Since these are sorted, just use the last element?
        if len(eviction_list) > 0 :
            max_clock = eviction_list[-1].Priority
            #also try max(eviction_list.Priority) ?
            self.Clock = max_clock

        return eviction_list

    ##############################################################

    def cache_miss(self, d:LambdaData, t, n, parallel=False):
        if not self.checkfree(d, n) : #due to space constraints
            #print("Eviction needed ", d.mem_size, self.mem_used)
            evicted = self.Eviction(d,n) #Is a list. also terminates the containers?

        num_cold, cap_miss = self.AddToPool(d,t,n, parallel)

        heapq.heapify(self.ContainerPool)
        return num_cold, cap_miss

    ##############################################################

    def cleanup_finished(self):
        """ Go through running containers, remove those that have finished """
        t = self.wall_time
        finished = []
        for c in self.running_c:
            (start_t, fin_t) = self.running_c[c]
            if t >= fin_t:
                finished.append(c)

        for c in finished:
            del self.running_c[c]
            if c.metadata.kind in self.histPrewarm and self.histPrewarm[c.metadata.kind] != 0:
                self.RemoveFromPool(c, "HIST-prewarm")

        heapq.heapify(self.ContainerPool)
        # We'd also like to set the container state to WARM (or atleast Not running.)
        # But hard to keep track of the container object references?
        return len(finished)

    ##############################################################

    def real_init_update(self, d: LambdaData):

        if self.real_init[d.kind][2] < 20:
            start_time = time.time()
            os.system("docker load -i ./image/image150ms.tar")
            end_time = time.time()
            end_time = start_time + (end_time - start_time)/10
        elif self.real_init[d.kind][2] < 35:
            start_time = time.time()
            os.system("docker load -i ./image/image150ms.tar")
            end_time = time.time()
            end_time = start_time + 2*(end_time - start_time)/10
        elif self.real_init[d.kind][2] < 50:
            start_time = time.time()
            os.system("docker load -i ./image/image150ms.tar")
            end_time = time.time()
            end_time = start_time + 3*(end_time - start_time)/10
        elif self.real_init[d.kind][2] < 65:
            start_time = time.time()
            os.system("docker load -i ./image/image150ms.tar")
            end_time = time.time()
            end_time = start_time + 4*(end_time - start_time)/10
        elif self.real_init[d.kind][2] < 80:
            start_time = time.time()
            os.system("docker load -i ./image/image150ms.tar")
            end_time = time.time()
            end_time = start_time + 5*(end_time - start_time)/10
        elif self.real_init[d.kind][2] < 95:
            start_time = time.time()
            os.system("docker load -i ./image/image150ms.tar")
            end_time = time.time()
            end_time = start_time + 6*(end_time - start_time)/10
        elif self.real_init[d.kind][2] < 110:
            start_time = time.time()
            os.system("docker load -i ./image/image150ms.tar")
            end_time = time.time()
            end_time = start_time + 7*(end_time - start_time)/10
        elif self.real_init[d.kind][2] < 200:
            start_time = time.time()
            os.system("docker load -i ./image/image150ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 300:
            start_time = time.time()
            os.system("docker load -i ./image/image250ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 400:
            start_time = time.time()
            os.system("docker load -i ./image/image350ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 500:
            start_time = time.time()
            os.system("docker load -i ./image/image450ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 600:
            start_time = time.time()
            os.system("docker load -i ./image/image550ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 700:
            start_time = time.time()
            os.system("docker load -i ./image/image650ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 800:
            start_time = time.time()
            os.system("docker load -i ./image/image750ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 900:
            start_time = time.time()
            os.system("docker load -i ./image/image850ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 1000:
            start_time = time.time()
            os.system("docker load -i ./image/image950ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 1200:
            start_time = time.time()
            os.system("docker load -i ./image/image1050ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 1400:
            start_time = time.time()
            os.system("docker load -i ./image/image1300ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 1650:
            start_time = time.time()
            os.system("docker load -i ./image/image1550ms.tar")
            end_time = time.time()
        elif self.real_init[d.kind][2] < 1950:
            start_time = time.time()
            os.system("docker load -i ./image/image1800ms.tar")
            end_time = time.time()
        else:
            start_time = time.time()
            os.system("docker load -i ./image/image2050ms.tar")
            end_time = time.time()

        os.system("docker rmi -f $(docker images -q)")
        self.real_init[d.kind][0] += 1
        self.real_init[d.kind][1] += (1000*(end_time - start_time))

    def runActivation(self, d: LambdaData, t = 0, n = 0):

        #First thing we want to do is queuing delays?
        #Also some notion of concurrency level. No reason that more cannot be launched with some runtime penalty...
        #Let's assume infinite CPUs and so we ALWAYS run at time t
        self.wall_time = t
        num_cold, cap_miss = self.AddToPool(d,t,n, parallel=False)
        n -= num_cold

        self.cleanup_finished()

        # Concurrency check can happen here. If len(running_c) > CPUs, put in the queue.
        # Could add fake 'check' entries corresponding to finishing times to check and drain the queue...

        idle_containres, warm_containers_to_reuse = self.find_container(d)

        I = len(idle_containres)
        B = len(warm_containers_to_reuse)

        if I >= n:
            for i in range(n): # all I idle
                c = idle_containres[i]
                c.run()
                processing_time = d.warm_time
                self.running_c[c] = (t, t+processing_time)
                self.WritePerfLog(d, 0, d.warm_time, "hit")
        else:
            for i in range(I): # all I idle
                c = idle_containres[i]
                c.run()
                processing_time = d.warm_time
                self.running_c[c] = (t, t+processing_time)
                self.WritePerfLog(d, 0, d.warm_time, "hit")

            m = 0
            while m < (min(B, n-I)): # all N' warm
                waiting_time = (self.running_c[warm_containers_to_reuse[m]][1] - t)
                if waiting_time < (d.run_time - d.warm_time): # waiting time is smaller, check next
                    c = warm_containers_to_reuse[m]
                    c.run()
                    processing_time = waiting_time + d.warm_time
                    self.running_c[c] = (t, t+processing_time)
                    self.WritePerfLog(d, waiting_time, d.warm_time, "hitreuse")
                    m += 1
                else: # waiting time is larger, m is the number of containers to reuse
                    break 

            if m < n-I:            
                num_cold, cap_miss = self.cache_miss(d, t, n-I-m, parallel=False)
                self.capacity_misses[d.kind] += cap_miss
                if random.random() < 0.25:
                    self.real_init_update(d)

        '''
        if c is None:
            if warm_containers_to_reuse == []: #No warm containers to reuse
                #Launch a new container since we didnt find one for the metadata ...
                c = self.cache_miss(d)
                #self.real_init_update(d)
                if c is None:
                    # insufficient memory
                    self.capacity_misses[d.kind] += 1
                    
                    #memusage = sum([k.metadata.mem_size for k in self.running_c.keys()])
                    return #memusage/self.mem_capacity
                c.run()
                #Need to update priority here?
                processing_time = d.run_time
                self.running_c[c] = (t, t+processing_time)
                self.WritePerfLog(d, d.run_time-d.warm_time, d.warm_time, "miss")

                #self.gantt.append((t, t+processing_time))
                #self.boxplot.append((d.run_time-d.warm_time)/d.run_time)

            elif (self.eviction_policy in ["MDP","FTC_R","FTC_M"]):
                #Compute the waiting time
                waiting_time = self.running_c[warm_containers_to_reuse[0]][1] - t
                if self.checkfree(d) or ((d.run_time - d.warm_time) < waiting_time): #If waiting time is longer
                    #Launch a new container since we didnt find one for the metadata ...
                    c = self.cache_miss(d)
                    #self.real_init_update(d)
                    if c is None:
                        # insufficient memory
                        self.capacity_misses[d.kind] += 1
                        
                        #memusage = sum([k.metadata.mem_size for k in self.running_c.keys()])
                        return #memusage/self.mem_capacity
                    c.run()
                    #Need to update priority here?
                    processing_time = d.run_time
                    self.running_c[c] = (t, t+processing_time)
                    self.WritePerfLog(d, d.run_time-d.warm_time, d.warm_time, "miss")

                    #self.gantt.append((t, t+processing_time))
                    #self.boxplot.append((d.run_time-d.warm_time)/d.run_time)

                else: #If waiting time is shorter
                    #Reuse a warm container
                    c = warm_containers_to_reuse[0]
                    c.run()
                    processing_time = waiting_time + d.warm_time
                    self.running_c[c] = (t, t+processing_time)
                    self.WritePerfLog(d, waiting_time, d.warm_time, "hitreuse") #reuse hit

                    #self.gantt.append((t, t+processing_time))
                    #self.boxplot.append((waiting_time)/processing_time)
            
            elif (self.eviction_policy in ["GD_R","LRU_R","FTC_S","BMO_R"]):
                #Parallel provision and reuse
                waiting_time = self.running_c[warm_containers_to_reuse[0]][1] - t
                if self.checkfree(d) or ((d.run_time - d.warm_time) < waiting_time): #If waiting time is longer
                    #Launch a new container since we didnt find one for the metadata ...
                    c = self.cache_miss(d)
                    #self.real_init_update(d)
                    if c is None:
                        # insufficient memory
                        self.capacity_misses[d.kind] += 1
                        
                        #memusage = sum([k.metadata.mem_size for k in self.running_c.keys()])
                        return #memusage/self.mem_capacity
                    c.run()
                    #Need to update priority here?
                    processing_time = d.run_time
                    self.running_c[c] = (t, t+processing_time)
                    self.WritePerfLog(d, d.run_time-d.warm_time, d.warm_time, "miss")

                else: #If waiting time is shorter
                    #Reuse a warm container
                    c = warm_containers_to_reuse[0]
                    c1 = self.cache_miss(d)
                    if c1 is not None:
                        c1.run()
                        processing_time_1 = d.run_time - d.warm_time
                        self.running_c[c1] = (t, t+processing_time_1)
                    else:
                        self.capacity_misses[d.kind] += 1
                        return
                    c.run()
                    processing_time = waiting_time + d.warm_time
                    self.running_c[c] = (t, t+processing_time)
                    self.WritePerfLog(d, waiting_time, d.warm_time, "hitreuse") #reuse hit

        else:
            # Strong assumption. If we can find the container, it is warm.
            c.run()
            processing_time = d.warm_time # d.run_time - d.warm_time
            self.running_c[c] = (t, t+processing_time)
            self.WritePerfLog(d, 0, d.warm_time, "hit")

            #self.gantt.append((t, t+processing_time))
            #self.boxplot.append((0))
            '''

        #update the priority here!!
        c = Container(d)
        c.last_access_t = self.wall_time
        new_prio = self.calc_priority(c, self.wall_time, self.BMO_trace) #, update=True)
        #Since frequency is cumulative, this will bump up priority of this specific container
        # rest of its clones will be still low prio. We should recompute all clones priority

        #for x in self.container_clones(c):
        #    x.Priority = new_prio

        for x in self.ContainerPool:
            if x.metadata == c.metadata:
                x.Priority = new_prio

        #Now rebalance the heap and update container access time
        #self.WritePureCacheHist(t)
        heapq.heapify(self.ContainerPool)
        
        #memusage = sum([k.metadata.mem_size for k in self.running_c.keys()])
        #return memusage/self.mem_capacity

    ##############################################################

    def miss_stats(self):
        """ Go through the performance log."""
        rdict = dict() #For each activation
        
        with open(self.PerfLogFName, "r") as f:
            line = f.readline() # throw away header
            for line in f:
                line = line.rstrip()
                d, coldtime, exectime, evtype = line.split(",")
                k = d
                if k not in rdict:
                    mdict = dict()
                    mdict['misses'] = 0
                    mdict['hits'] = 0
                    mdict['cold'] = 0
                    mdict['exec'] = 0
                    mdict['hitreuse'] = 0
                    mdict['waiting'] = 0
                    rdict[k] = mdict

                rdict[k]['cold'] = rdict[k]['cold'] + float(coldtime)
                rdict[k]['exec'] = rdict[k]['exec'] + float(exectime)

                if evtype == "miss":
                    rdict[k]['misses'] = rdict[k]['misses'] + 1
                elif evtype == "hit":
                    rdict[k]['hits'] = rdict[k]['hits'] + 1
                elif evtype == "hitreuse":
                    rdict[k]['hitreuse'] = rdict[k]['hitreuse'] + 1
                    rdict[k]['waiting'] = rdict[k]['waiting'] + float(coldtime)
                else:
                    pass
        #Also some kind of response time data?
        return rdict

    ##############################################################
    ##############################################################
    ##############################################################

if __name__ == "__main__":
    from pprint import pprint
    #from TestTraces import *
    ls = LambdaScheduler(policy="TTL", mem_capacity=1024, num_funcs=10, run="b")
    # lt = LowWellTrace()
    # lambdas, input_trace = lt.gen_full_trace(1, sample_seed=1)

    pth = "/extra/alfuerst/azure/functions/trace_pckl/bottom_qt/10-b.pckl"
    with open(pth, "r+b") as f:
        lambdas, input_trace = pickle.load(f)
    print(len(input_trace))

    # for d, t in input_trace:
    #     print(d, t/1000)
    
    for d, t in input_trace:
        ls.runActivation(d, t)

    print("\n\nDONE\n")

    pprint(ls.evdict)
    pprint(ls.miss_stats())
    print("cap", ls.capacity_misses)

    # print(ls.IT_histogram)
    # print(ls.wellford, "\n")
    # print("hist-ttl",ls.histTTL)
    # print("last seen", ls.last_seen)
    # print("prewarm", ls.histPrewarm)


    # for key, value in ls.wellford.items():
    #     mean, variance, sampleVariance = ls.well_finalize(value)
    #     if isnan(mean):
    #         continue
    #     if variance != 0:
    #         if mean < 0:
    #             print(ls.IT_histogram[key])
    #         print(key, mean, variance, sampleVariance, mean/variance)
    #     else:
    #         print(mean, variance, sampleVariance)