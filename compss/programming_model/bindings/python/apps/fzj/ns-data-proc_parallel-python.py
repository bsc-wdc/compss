



import pp
import time
import sys
import pickle
from numpy import * 

#load experiment and analysis parameters
from experiment_params import num_neurons,num_secs,num_bins
from analysis_params import maxlag,num_surrs

#cc_surrogate range calculates cc and surrogate cc for a given range of indices
def cc_surrogate_range(start_idx, end_idx, seed, num_neurons, num_surrs, num_bins, maxlag):
    numpy.random.seed(seed)
    f = open('./spikes.dat', 'r')
    spikes = pickle.load(f)
    idx = 0
    row = 0
    my_cc_original = numpy.zeros((end_idx-start_idx,2*maxlag+1))
    my_cc_surrs = numpy.zeros((end_idx-start_idx,2*maxlag+1,2))
    idxrange = range(num_bins-maxlag,num_bins+maxlag+1)
    surrs_ij = numpy.zeros((num_surrs,2*maxlag+1))
    for ni in numpy.arange(num_neurons-1):
        for nj in range(ni+1,num_neurons):
            #get to first index of relevant range
            if (idx < start_idx):
                idx = idx + 1
                continue
            #calculate cc and surrogate ccs for all indices in relevant range
            elif (idx < end_idx):
                my_cc_original[row,:] = numpy.correlate(spikes[ni,:],spikes[nj,:],"full")[idxrange]
                num_spikes_i = numpy.sum(spikes[ni,:])
                num_spikes_j = numpy.sum(spikes[nj,:])
                for surrogate in range(num_surrs):
                    surr_i = numpy.zeros(num_bins)
                    surr_i[numpy.random.random_integers(0,num_bins-1,num_spikes_i)] = 1
                    surr_j = numpy.zeros(num_bins)
                    surr_j[numpy.random.random_integers(0,num_bins-1,num_spikes_j)] = 1
                    surrs_ij[surrogate,:] = numpy.correlate(surr_i,surr_j,"full")[idxrange]
                #save point-wise 5% and 95% values of sorted surrogate ccs 
                surrs_ij_sorted = numpy.sort(surrs_ij,axis=0)
                my_cc_surrs[row,:,0] = surrs_ij_sorted[round(num_surrs*0.95),:]
                my_cc_surrs[row,:,1] = surrs_ij_sorted[round(num_surrs*0.05),:]
                idx = idx + 1
                row = row + 1
            #reached end of relevant range; return results
            else:  
                return [my_cc_original, my_cc_surrs]
    
    return [my_cc_original, my_cc_surrs]

#secret
scrt='mysecret'

# tuple of all parallel python servers to connect with
ppservers = ('comp1.my-network', 'comp2.my-network', 'comp3.my-network')

if len(sys.argv) > 1:
    ncpus = int(sys.argv[1])
    #creates jobserver with ncpus workers
    job_server = pp.Server(ncpus, ppservers=ppservers,  loglevel=0, secret=scrt)
else:
    #creates jobserver with automatically detected number of workers
    job_server = pp.Server(ppservers=ppservers,  loglevel=0, secret=scrt)
    
#wait for servers to come up
time.sleep(5)

#calculate number of nodes in total
nlocalworkers = job_server.get_ncpus()
activenodes=job_server.get_active_nodes()
workerids=activenodes.keys()
nworkers=sum( [activenodes[workerids[i]] for i in range(len(workerids))] ) + nlocalworkers
print nworkers

num_ccs = (num_neurons**2 - num_neurons)/2
#calculate number of pairs each worker should process
step = ceil(float(num_ccs)/nworkers)
start_idx = 0
end_idx = 0
starts = zeros((nworkers+1,))
starts[-1] = num_ccs

seed = 2398645
delta = 1782324
jobs = []

#send out jobs
for worker in range(nworkers):
    start_idx = end_idx
    end_idx = int(min((worker+1)*step,num_ccs)) 
    print start_idx, " -> ", end_idx - 1
    starts[worker] = start_idx
    params = start_idx, end_idx, seed, num_neurons, num_surrs, num_bins, maxlag
    depfuncs = ()
    depmodules = "numpy","pickle",
    jobs.append(job_server.submit(cc_surrogate_range,params,depfuncs,depmodules))
    seed = seed + delta
    
print "submitted all jobs"
#collect results from workers
cc_original = zeros((num_ccs,2*maxlag+1))
cc_surrs = zeros((num_ccs,2*maxlag+1,2))
for worker in arange(nworkers):
    start = starts[worker]
    end = starts[worker + 1]
    result = jobs[worker]()
    cc_original[start:end,:] = result[0]
    cc_surrs[start:end,:,:] = result[1]

#save results
f = open('./result_cc_originals.dat','w')
pickle.dump(cc_original,f)
f.close()
f = open('./result_cc_surrogates_conf.dat','w')
pickle.dump(cc_surrs,f)
f.close()
