#!/usr/bin/sh

# Requires:
# 	-> module load ScoreP_lite/3.0_gcc TAU_lite/2.26.1_gcc CUBE/4.3.4_gcc

set -e

#tau_dir=/lustre/software/TAU_lite/2.26.1_gcc/x86_64/lib/bindings-python-scorep/

#export LD_LIBRARY_PATH="$tau_dir:$LD_LIBRARY_PATH"

#export PYTHONPATH="$tau_dir:$PYTHONPATH"

export SCOREP_DIR=$(realpath "$(dirname $(which scorep-config))/../")

LIBTOOL="/usr/bin/libtool --silent"

CC=gcc

INSTALL=/usr/bin/install

scorep_preload_name="scorep_preload"

scorep_preload_dir=".scorep_preload"

print_help()
{
    cat <<EOH
Usage
=====
1. Build the Score-P preload:
> $0 build --mpp=<none|mpi|shmem> /path/to/application

2. Get the preload for the Score-P libraries
> $0 print /path/to/application

You can run your MPI application like this
> LD_PRELOAD=\$($0 print /path/to/application) mpirun -np 4 helloworld

Please, be careful when you use a job scheduler like SLURM or PBS
because they have specific options/variables to set the LD_PRELOAD!

*.Cleaning the workspace
> rm -rf /path/to/application/.scorep_preload
EOH
}

build()
{
	if [ $# -lt 3 ];then
           	echo "Wrong number of arguments"
	  	print_help
		exit -1
        fi

	scorep_preload_dir=$(realpath $3/$scorep_preload_dir)

	if [ -d "$scorep_preload_dir" ]; then
	    	echo "Preload folder already exists!"
	    	echo "For re-building, delete the $scorep_preload_dir folder"
		exit -1
	fi
	
	mkdir -p "$scorep_preload_dir"

	scorep_init_options="$2 --user --nocompiler"

	echo "$scorep_init_options" > "$scorep_preload_dir/scorep_init_options.conf"

	scorep_tmp="$(mktemp -d -t scorep_preload.XXXXXXXXXX)"

	scorep-config $scorep_init_options --adapter-init > "$scorep_tmp/$scorep_preload_name.c"

        ${LIBTOOL} --mode=compile --tag=CC \
		${CC} \
		-c \
		-shared \
		-o "$scorep_tmp/$scorep_preload_name.lo" \
		"$scorep_tmp/$scorep_preload_name.c"

	${LIBTOOL} --mode=link --tag=CC \
		${CC} \
		-aviod-version \
		-module \
		-shared \
		-o "$scorep_tmp/$scorep_preload_name.la" \
		"$(scorep-config $scorep_init_options --ldflags)" \
		"$scorep_tmp/$scorep_preload_name.lo" \
		-rpath "$scorep_preload_dir"

	${LIBTOOL} --mode=install \
		$INSTALL "$scorep_tmp/$scorep_preload_name.la" \
		"$scorep_preload_dir/$scorep_preload_name.la"

	echo "Build Score-P preload in $scorep_preload_dir"

	echo "You can set the preload like this:"
	echo "LD_PRELOAD=\$\($0 print /path/to/application)"

	rm -rf "$scorep_tmp"
}

print_preload()
{
	if [ $# -lt 2 ]; then
		echo "Wrong number of arguments."
		print_help
		exit -1
	fi	

	scorep_preload_dir=$(realpath $2/$scorep_preload_dir)
	
	if [ ! -d "$scorep_preload_dir" ] || [ ! -f "$scorep_preload_dir/scorep_init_options.conf" ]; then
		echo "No Score-P preload folder found in $2"
	        print_help
		exit -1
	fi

	scorep_init_options=$(cat $scorep_preload_dir/scorep_init_options.conf)
	preload_str="$scorep_preload_dir/$scorep_preload_name.so"
	scorep_subsystems=$(scorep-config $scorep_init_options --libs | tr ' ' '\n' | grep -o "scorep.*")
	for i in $scorep_subsystems
	do
		preload_str="$preload_str:$SCOREP_DIR/lib/lib$i.so"
	done
	echo "$preload_str"
}

if [ "$1" == "build" ]; then
    build "$@"
elif [ "$1" == "print" ]; then
    print_preload "$@"
elif [ "$1" == "help" ]; then
    print_help
fi
