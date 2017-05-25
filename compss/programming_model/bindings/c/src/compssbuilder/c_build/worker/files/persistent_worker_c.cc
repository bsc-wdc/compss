
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

string END_TASK_TAG = "endTask";
string QUIT_TAG = "quit";
string EXECUTE_TASK_TAG = "task";

/*
int readline(int fd, string& command, int& start) {
    char c;
    char buffer[4096];
    int counter = 0;
   
    cout << "reading from " << start << endl;
    lseek(fd, start, SEEK_SET);
    while (read(fd, &c, 1)) {
        if (c == '\n') {
            break;
        }
        buffer[counter++] = c;
    }
    string aux(buffer);
    command = aux;
    start = counter;
    cout << "counter is " << counter << endl;
    cout << "read string is " << command << endl;
    return counter;
}*/


string readline(const char* inPipe) {
	cout << "OPENING PIPE THREAD " << boost::this_thread::get_id() << endl;
	fflush(NULL);
	ifstream inFile;
	inFile.open( inPipe , ios::in);

//nt fd = open(inPipe, O_RDONLY);

        cout << "READING LINE THREAD " << boost::this_thread::get_id() << endl;
	fflush(NULL);
	string command;
//	getline(inFile,command);
/*
	char c;
    char buffer[4096];
    int counter = 0;

    while (read(fd, &c, 1)) {
        if (c == '\n') {
            break;
        }
        buffer[counter++] = c;
    }
    string aux(buffer);
    command = aux;

	cout << "NUMBER OF READ CHARACTERS: " << counter << endl;
	fflush(NULL);
*/
	while(!getline(inFile,command)){
		cout << "WAITING FOR COMMAND THREAD " << boost::this_thread::get_id() << endl;
		inFile.close();
		fflush(NULL);
		usleep(1000);
		inFile.open(inPipe , ios::in);		
	}

//	cout << "CLOSING FILE, RET VALUE IS " << ret << endl;
	inFile.close();
	//close(fd);
	cout << "READ COMMAND IS \'" << command << "\'" << endl;
	return command;
}



void runThread(const char* inPipe, const char* outPipe){

	printf("Hello world\n");
        fflush(NULL);

        ofstream outFile;
        outFile.open(outPipe);

	
	//int fd = open(inPipe, O_WRONLY);
	



//	ifstream inFile;
 //       inFile.open( inPipe , ios::in);
//
//	vector<string> commandArgs;
//	vector<string> executeArgs;
//	char** executeArgsC;

        string command;
	string output;

	while(true){
		
		//getline(inFile, command);
		command = readline(inPipe);
/*
		if (inFile.eof()) {
			cout <<" READ EOF" << endl;
			fflush(NULL);
			inFile.close();
			outFile.close();
			cout <<"TRYING TO OPEN" << endl;
                        fflush(NULL);
			inFile.open(inPipe, ios::in);
			outFile.open(outPipe);
			cout <<"OPENED" << endl;
                        fflush(NULL);
		}
*/
	        cout << "getting the line" << endl;
		//ut << "THE COMMAND IS: " << command << endl;

		cout << "THE THREAD IS IS " << boost::this_thread::get_id() << endl;

		if (command == QUIT_TAG) {
			cout << "QUIT RECEIVED" << endl;
			break;
		}
		string aux;
                stringstream ss(command);

		vector<string> commandArgs;
        	vector<string> executeArgs;
        	char** executeArgsC;

                while (ss >> aux){
                        commandArgs.push_back(aux);
                }

                for (int i = 0; i < commandArgs.size(); i++) cout << "ARG " << i << " IS " << commandArgs[i] << endl;

                for (int i = 0; i < commandArgs.size(); i++) {   
                        int pos = commandArgs[i].find("worker_c");         
                        if (pos != -1){
				cout << "FOUND WORKER_C in position " << i  <<  endl;
                                executeArgs = vector<string>(commandArgs.begin() + i, commandArgs.end());
				cout << "THE SIZE IS " << executeArgs.size();
                        }
                }

		//executeArgs[2] = commandArgs[1];

		ofstream job_out(commandArgs[2].c_str());
		job_out << "This is a sample output.\n";
		job_out.close();	

		ofstream job_err(commandArgs[3].c_str());
                job_err << "This is a sample error.\n";
                job_err.close();

		executeArgsC = new char*[executeArgs.size()];

		for (int i = 0; i < executeArgs.size(); i++){
			cout << "EXECUTE ARG " << i << " IS " << executeArgs[i] << endl;
			executeArgsC[i] = new char[executeArgs[i].size() + 1];
			strcpy(executeArgsC[i], executeArgs[i].c_str());
		}

		int ret = execute(executeArgs.size(), executeArgsC);

		ostringstream out_ss;
		out_ss << END_TASK_TAG << " " << commandArgs[1] << " " << ret << endl;
		output = out_ss.str();

		cout << "THE OUTPUT IS: " << output << endl;

                outFile << output;
		fflush(NULL);
		outFile.close();
		outFile.open(outPipe);
		
        }

	outFile.close();
	cout << "THREAD " << boost::this_thread::get_id() << " QUITTING" << endl;

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
   
    cout << "THE TAGS ARE:" << endl;
    cout << "END_TASK_TAG: " << END_TASK_TAG << endl;
    cout << "QUIT_TAG: " << END_TASK_TAG << endl;
    cout << "EXECUTE_TASK_TAG: " << END_TASK_TAG << endl;

 
    //Add here treads stuff

    boost::asio::io_service ioService;
    boost::thread_group threadpool;

    boost::asio::io_service::work work(ioService);
    for (int i = 0; i < numInPipes; i++){
            threadpool.create_thread(
                    boost::bind(runThread, inPipes[i], outPipes[i])
            );
    }
    threadpool.join_all();
 
    cout << "ABOUT TO RETURN" << endl;
   
    return 0;
} 
