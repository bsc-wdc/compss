GCC=g++
CFLAGS=-std=c++0x -g -fpermissive -I. -I/apps/COMPSs/Trunk2/Bindings/c/include -I/apps/COMPSs/Trunk2/Bindings/bindings-common/include -I/apps/BOOST/1.57/include 

CFILES=Block.cc Matrix.cc
C_OBJ=Block.o Matrix.o


all: clean compile_master compile_worker deploy

compile_master:
	@echo "Building application for master..."
	$(GCC) -DCOMPSS_MASTER $(CFLAGS) -c $(CFILES)
	ar rvs libmaster.a $(C_OBJ)
	ranlib libmaster.a

compile_worker:
	@echo "Building application for workers..."
	$(GCC) -DCOMPSS_WORKER $(CFLAGS) -c Block.cc
	$(GCC) -DCOMPSS_WORKER $(CFLAGS) -c Matrix.cc 
	ar rvs libworker.a $(C_OBJ)
	ranlib libworker.a

deploy:
	@echo "Deploying lib files..."
	@rm -rf ../lib
	@mkdir ../lib
	@mv *.a ../lib

clean:
	@echo "Cleaning intermediate files..."
	@rm -f *.o

