#include "executor.h"

#include <boost/asio/io_service.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
//#include <pthread.h>
#include <sys/types.h>
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)

using namespace std;

map<string, void*> cache;

string END_TASK_TAG = "endTask";
string QUIT_TAG = "quit";
string EXECUTE_TASK_TAG = "task";

//Reads a command when the other end of the pipe is written
string readline(const char* inPipe) {
	ifstream inFile;
	inFile.open( inPipe , ios::in);

	string command;
	getline(inFile,command);
	inFile.close();
	
	return command;
}


streambuf * redirect_error(const char * filenm, ofstream& filestr)
{
  streambuf *newsb, *oldsb;
  filestr.open(filenm);
  oldsb = cerr.rdbuf();     // back up cout's streambuf
  newsb = filestr.rdbuf();       // get file's streambuf
  cerr.rdbuf(newsb);        // assign streambuf to cout
  return oldsb;
}


void restore_error(streambuf * oldsb, ofstream& filestr)
{
  cerr.rdbuf(oldsb);        // restore cout's original streambuf
  filestr.close();
}



streambuf * redirect_output(const char * filenm, ofstream& filestr)
{
  streambuf *newsb, *oldsb;
  filestr.open(filenm);
  oldsb = cout.rdbuf();     // back up cout's streambuf
  newsb = filestr.rdbuf();       // get file's streambuf
  cout.rdbuf(newsb);        // assign streambuf to cout
  return oldsb;
}


void restore_output(streambuf * oldsb, ofstream& filestr)
{
  cout.rdbuf(oldsb);        // restore cout's original streambuf
  filestr.close();
}


void runThread(const char* inPipe, const char* outPipe){

    ofstream outFile, jobOut, jobErr;
	streambuf *oldOutsb, *oldErrsb;
    string command;
	string output;

	while(true){
		
		command = readline(inPipe);
		
		if (command != ""){

			if (command == QUIT_TAG) {
				cout << "[Persistent C] Quit received" << endl;
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

            for (int i = 0; i < commandArgs.size(); i++) {   
               	int pos = commandArgs[i].find("worker_c");         
               	if (pos != -1){
                   	executeArgs = vector<string>(commandArgs.begin() + i, commandArgs.end());
               	}
            }

			oldOutsb = redirect_output(commandArgs[2].c_str(), jobOut);
            oldErrsb = redirect_error(commandArgs[3].c_str(), jobErr);

			
			for (int i = 0; i < commandArgs.size(); i++) {
				if (commandArgs[i] == "taskset"){
					cpu_set_t to_assign;
					CPU_ZERO(&to_assign);
					string assignedCpuString = commandArgs[i+2];
					vector<int>assignedCpus;
					stringstream ss_cpus(assignedCpuString);
					int cpu;
					//Read integers from the list of cpus assigned, ignore commas
					while (ss_cpus >> cpu){
						CPU_SET(cpu, &to_assign);
						if (ss_cpus.peek() == ',') ss_cpus.ignore();
					}
				
					if(sched_setaffinity(gettid(), sizeof(cpu_set_t), &to_assign) < 0) {
    					cout << "[Persistent C] Error during sched_setaffinity call!" << endl;
  					}


				}
			}


			executeArgsC = new char*[executeArgs.size()];

			for (int i = 0; i < executeArgs.size(); i++){
				executeArgsC[i] = new char[executeArgs[i].size() + 1];
				strcpy(executeArgsC[i], executeArgs[i].c_str());
			}

			int ret = execute(executeArgs.size(), executeArgsC, cache);

			restore_output(oldOutsb, jobOut);
			restore_error(oldErrsb, jobErr);

			ostringstream out_ss;
			out_ss << END_TASK_TAG << " " << commandArgs[1] << " " << ret << endl;
			output = out_ss.str();

			outFile.open(outPipe);
            outFile << output;
			fflush(NULL);
			outFile.close();
	
		}
    }

	cout << "[Persistent C] Thread " << boost::this_thread::get_id() << " quitting with output " << output << endl;

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

	
   
    boost::asio::io_service ioService;
    boost::thread_group threadpool;

    boost::asio::io_service::work work(ioService);
    for (int i = 0; i < numInPipes; i++){
            threadpool.create_thread(
                    boost::bind(runThread, inPipes[i], outPipes[i])
            );
    }
    threadpool.join_all();
 
    cout << "[Persistent C] Worker shutting down" << endl;
   
    return 0;
} 
