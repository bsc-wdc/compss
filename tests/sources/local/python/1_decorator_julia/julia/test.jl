#using Distributed, ClusterManagers

# PyCOMPSs requires overlap y mem=0
#addprocs_slurm(2)   # Fails
#addprocs_slurm(96)  # Fails
#addprocs_slurm(2, overlap="", mem="0")
#addprocs_slurm(96, overlap="", mem="0")
# This could be used to gather the number of tasks set by the @julia decorator
#addprocs_slurm(parse(Int, ENV["SLURM_NTASKS"]), overlap="", mem="0")

#@everywhere using Distributed
#@everywhere println(myid())
#@everywhere println(gethostname())

println("hellow world - julia success")
