./buildsc 4.0.0-alpha
cd ../../packages/sc/
scp COMPSs_4.0.0-alpha.tar.gz ns54:.
ssh ns54 ./build_compss.sh 
