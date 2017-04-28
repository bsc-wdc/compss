
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include "executor.h"
#include <fstream>

#include <iostream>
#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>

using namespace std;

void runThread(const char* inPipe, const char* outPipe){

/*
        int value;
        string filename;
        int opCod;

        while(cin >> opCod >> filename >> value){
                //
                // tag taskset -c ts_args worker_c worker_c_args[]...
                //if (tag == END_TAG) break;
                // if tag EXECUTE_TASK
                //1-ts_args -> threadAffinity (bitmask)

                // Tot a una int exitcode = execute_task(worker_c_args)
                //2- parse opId + params

                if (opCod == -1) break;
                cout << "id is " << boost::this_thread::get_id() << endl;
                int ret = execute(opCod, objectStorage, value, filename);
                sleep(5);
                //int ret = pipeWrite(params);

        }
*/

	printf("Hello world\n");
        fflush(NULL);

        ifstream inFile( inPipe , ios::in);
        ofstream outFile;
        outFile.open(outPipe);

        string command;
        string end = "TASK_END 0 1\n";
        while(getline(inFile, command)){
                printf("getting the line\n");
                fflush(NULL);
                //inFile.getline(s,200000);
                //getline(inFile, command);
                cout << command << endl;
                fflush(NULL);
                outFile << end;
        }

        outFile.close();
        inFile.close();	
}






int main(int argc, char **argv) {
    if (argc < 4) {
        printf("ERROR: Incorrect number of COMPSs internal parameters\n");
        printf("Aborting...\n");
        return -1;
    }
    //Reading in pipes
    printf("numInPipes %s.\n", argv[1]);
    int numInPipes=atoi(argv[1]);
    printf("Detected %d in pipes.\n", numInPipes);
    char* inPipes[numInPipes];
    for (int i=0; i<numInPipes; i++){
        inPipes[i] = argv[i+2];
        printf("In pipe %d: %s\n",i, inPipes[i]);
    }
    //Reading out pipes
    printf("numInPipes %s.\n", argv[numInPipes+2]);
    int numOutPipes=atoi(argv[numInPipes+2]);
    printf("Detected %d out pipes.\n", numOutPipes);
    char* outPipes[numOutPipes];
    for (int i=0; i<numOutPipes; i++){
        outPipes[i] = argv[i+numInPipes+3];
	printf("Out pipe %d: %s\n",i, outPipes[i]);
    }

    fflush(NULL);
    
    //Add here treads stuff

	boost::asio::io_service ioService;
        boost::thread_group threadpool;

        boost::asio::io_service::work work(ioService);
        //COMPROVAR ARGS
        //if argsc >
        // PARSE cmdPipes resultPipes
        //char** cmdpipes, resultpipes;
        //Parse NTHREADS size cmdpipes
        for (int i = 0; i < numInPipes; i++){
                threadpool.create_thread(
                        boost::bind(runThread, inPipes[i], outPipes[i])
                );
                //boost::bind(runThread(cmdpipes[i],resultpipes[i])
        }
        threadpool.join_all();
    
    return 0;
} 
