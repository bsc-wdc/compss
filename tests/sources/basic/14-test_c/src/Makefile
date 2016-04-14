all:

	@echo "Building application..."

	g++ -DCOMPSS_MASTER -DDEBUG_BINDING -g -I. -I/opt/COMPSs/Runtime/bindings/c/include -I/opt/COMPSs/Runtime/bindings/bindings-common/include -c address.cc student.cc
	#g++ -DCOMPSS_MASTER -g -I. -I/opt/COMPSs/Runtime/bindings/c/include -I/opt/COMPSs/Runtime/bindings/bindings-common/include -c address.cc student.cc
		
	ar rvs libmaster.a address.o student.o
	ranlib libmaster.a
	
	rm *.o
		
	g++ -DCOMPSS_WORKER -DDEBUG_BINDING -g -I. -I/opt/COMPSs/Runtime/bindings/c/include -I/opt/COMPSs/Runtime/bindings/bindings-common/include -c address.cc student.cc
	#g++ -DCOMPSS_WORKER -g -I. -I/opt/COMPSs/Runtime/bindings/c/include -I/opt/COMPSs/Runtime/bindings/bindings-common/include -c address.cc student.cc
		
	ar rvs libworker.a address.o student.o
	ranlib libworker.a 
	
	rm *.o		
	 			
	@rm -rf ../lib
	@mkdir ../lib
	@mv *.a ../lib
	
	@echo " " 
