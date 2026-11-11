./buildsc 4.0.0-beta.1
cd ../../packages/sc/
scp COMPSs_4.0.0-beta.1.tar.gz ns54:.
ssh ns54 ./build_compss.sh 
