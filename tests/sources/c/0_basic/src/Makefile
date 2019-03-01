GCC=g++
#DEBUG_FLAG=-DDEBUG_BINDING
CFLAGS=${DEBUG_FLAG} -g -I. -I/opt/COMPSs/Bindings/c/include -I/opt/COMPSs/Bindings/bindings-common/include

all: clean compile_master compile_worker deploy

compile_master:
	@echo "Building application for master..."
	$(GCC) -DCOMPSS_MASTER $(CFLAGS) -c address.cc student.cc
	ar rvs libmaster.a address.o student.o
	ranlib libmaster.a

compile_worker:
	@echo "Building application for workers..."
	$(GCC) -DCOMPSS_WORKER $(CFLAGS) -c address.cc student.cc
	ar rvs libworker.a address.o student.o
	ranlib libworker.a

deploy:
	@echo "Deploying lib files..."
	@rm -rf ../lib
	@mkdir ../lib
	@mv *.a ../lib

clean:
	@echo "Cleaning intermediate files..."
	rm -f *.o

