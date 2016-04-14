



import pickle
from numpy import *

#load experiment and analysis parameters
from experiment_params import num_neurons,num_secs,num_bins
from analysis_params import maxlag,num_surrs

from pycompss.api.task import task
from pycompss.api.parameter import *

@task(originals_file = FILE_INOUT, surrs_file = FILE_INOUT)
def gather(result, originals_file, surrs_file, start, end):
    cc_original = result[0]
    f = open(originals_file, 'a')
    pickle.dump(cc_original, f)
    f.close()
    # start, end ??

    cc_surrs = result[1]
    f = open(surrs_file, 'a')
    pickle.dump(cc_surrs, f)
    f.close()
    # start, end ??

#cc_surrogate range calculates cc and surrogate cc for a given range of indices
@task(returns = list)
def cc_surrogate_range(start_idx, end_idx, seed, num_neurons, num_surrs, num_bins, maxlag):
    random.seed(seed)
    f = open('./spikes.dat', 'r')
    spikes = pickle.load(f)
    idx = 0
    row = 0
    my_cc_original = zeros((end_idx-start_idx,2*maxlag+1))
    my_cc_surrs = zeros((end_idx-start_idx,2*maxlag+1,2))
    idxrange = range(num_bins-maxlag,num_bins+maxlag+1)
    surrs_ij = zeros((num_surrs,2*maxlag+1))
    for ni in arange(num_neurons-1):
        for nj in range(ni+1,num_neurons):
            #get to first index of relevant range
            if (idx < start_idx):
                idx = idx + 1
                continue
            #calculate cc and surrogate ccs for all indices in relevant range
            elif (idx < end_idx):
                my_cc_original[row,:] = correlate(spikes[ni,:],spikes[nj,:],"full")[idxrange]
                num_spikes_i = sum(spikes[ni,:])
                num_spikes_j = sum(spikes[nj,:])
                for surrogate in range(num_surrs):
                    surr_i = zeros(num_bins)
                    surr_i[random.random_integers(0,num_bins-1,num_spikes_i)] = 1
                    surr_j = zeros(num_bins)
                    surr_j[random.random_integers(0,num_bins-1,num_spikes_j)] = 1
                    surrs_ij[surrogate,:] = correlate(surr_i,surr_j,"full")[idxrange]
                #save point-wise 5% and 95% values of sorted surrogate ccs 
                surrs_ij_sorted = sort(surrs_ij,axis=0)
                my_cc_surrs[row,:,0] = surrs_ij_sorted[round(num_surrs*0.95),:]
                my_cc_surrs[row,:,1] = surrs_ij_sorted[round(num_surrs*0.05),:]
                idx = idx + 1
                row = row + 1
            #reached end of relevant range; return results
            else:  
                return [my_cc_original, my_cc_surrs]
    
    return [my_cc_original, my_cc_surrs]


if __name__ == "__main__":
    import sys
    from pycompss.api.api import compss_wait_on

    num_frags = int(sys.argv[1])
        
    num_ccs = (num_neurons**2 - num_neurons)/2
    step = ceil(float(num_ccs)/num_frags)
    start_idx = 0
    end_idx = 0
    
    seed = 2398645
    delta = 1782324
    results = []
    
    #send out tasks
    originals_file = 'result_cc_originals.dat'
    surrs_file = 'result_cc_surrogates_conf.dat'
    for frag in range(num_frags):
        start_idx = end_idx
        end_idx = int(min((frag+1)*step,num_ccs)) 
        print start_idx, " -> ", end_idx - 1
        result = cc_surrogate_range(start_idx, end_idx, seed, num_neurons, num_surrs, num_bins, maxlag)
        gather(result, originals_file, surrs_file, start_idx, end_idx)
        seed = seed + delta
        
    print "submitted all tasks"
    
