export DEPS_PATH=/fefs/scratch/bsc19/bsc19776/

#with virtual environment
#export DEPS_PATH=/fefs/scratch/bsc19/bsc19776/
#source ${DEPS_PATH}/opt/virtual_python-3.6.8/bin/activate
#export PATH=/opt/FJSVxtclanga/tcsds-1.2.26/bin:$PATH
#export LD_LIBRARY_PATH=$PYTHON/lib:/opt/FJSVxtclanga/tcsds-1.2.26/lib64:${DEPS_PATH}/opt/cblas:$LD_LIBRARY_PATH
#export EXTRAE_MPI_HEADERS=/opt/FJSVxtclanga/tcsds-1.2.26/include/mpi/fujitsu
#export EXTRAE_MPI_LIBS=/opt/FJSVxtclanga/tcsds-1.2.26/lib64

#with installed python
module load fuji/1.2.26b python/3.6.8
export EXTRAE_MPI_HEADERS=/opt/FJSVxtclanga/tcsds-1.2.26b/include/mpi/fujitsu
export EXTRAE_MPI_LIBS=/opt/FJSVxtclanga/tcsds-1.2.26b/lib64

#COMMON
export CC=gcc
#export CXX=g++
export MPICC=mpifcc
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.222.b10-1.el8.aarch64
export BOOST_INCLUDE=${DEPS_PATH}/opt/boost_1_73_0
export BOOST_LIB=${DEPS_PATH}/opt/boost_1_73_0/libs
