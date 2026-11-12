./buildsc 4.0.0-beta.2
cd ../../packages/sc/
scp COMPSs_4.0.0-beta.2.tar.gz ns54:.
ssh ns54 ./build_compss.sh 
