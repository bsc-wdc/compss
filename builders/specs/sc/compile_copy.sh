./buildsc 3.4
cd ../../packages/sc/
scp COMPSs_3.4.tar.gz ns54:.
ssh ns54 ./build_compss.sh 
