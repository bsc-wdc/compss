#include "executor.h"
#define gettid() syscall(SYS_gettid)

using namespace std;

map<string, void*> cache;
map<string,int> types;

string END_TASK_TAG = "endTask";
string QUIT_TAG = "quit";
string EXECUTE_TASK_TAG = "task";
string REMOVE_TAG = "remove";
string SERIALIZE_TAG = "serialize";

int endedThreads;

pthread_mutex_t mtx;

struct arg_t {
	char * inPipe;
	char * outPipe;
	char * inDataPipe;
	char * outDataPipe;
	customStream *csOut;
	customStream *csErr;
	int nThreads;
};


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


bool checkDataToRead(const char* inDataPipe){
	cout << "before open" << endl;	
	int fd = open(inDataPipe, O_RDONLY);
	int flags = fcntl(fd, F_GETFL, 0);
	fcntl(fd, F_SETFL, flags | O_NONBLOCK);

	char c;
	int res = read(fd, &c, 1);

	close(fd);
	
	cout << "result to be returned is " << res << endl;

	if (res >= 0) {
		cout << "something has been read" << endl;
		return true;
	}
	cout << "there is nothing to read" << endl;

	return false;
}

void *runThread(void * arg){

	cout << "start run thread" << endl;

#ifdef OMPSS_ENABLED
    nanos_admit_current_thread();
    cout << "enabled" << endl;
#endif


//	pthread_mutex_t mtx;

    ofstream outFile;
    string command;
	string output;

	char *inPipe, *outPipe;
	char *inDataPipe, *outDataPipe;

	struct arg_t *args = (struct arg_t *)arg;

	inPipe = args->inPipe;
	outPipe = args->outPipe;
	inDataPipe = args->inDataPipe;
	outDataPipe = args->outDataPipe;
	customStream *csOut = args->csOut;
	customStream *csErr = args->csErr;
	int nThreads = args->nThreads;

	while(true){
/*
		if (checkDataToRead(inDataPipe)){
			cout << "check succesful" << endl;
			command = readline(inDataPipe);
			cout << "command read is " << command << endl;
		}
*/
		cout << "thread expecting to read command" << endl;

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

			ofstream * jobOut = new ofstream(commandArgs[2].c_str());
			ofstream * jobErr = new ofstream(commandArgs[3].c_str());
						
			csOut->registerThread(jobOut->rdbuf());
			csErr->registerThread(jobErr->rdbuf());
			
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

			for (int i = 0; i < 5; i++){
				cout << executeArgs[i+14] << endl;
			}

			cout << "before execute" << endl;
			//last integer indicates if output data is going to be serialized at the end of the task execution 0=no 1=yes 
			int ret = execute(executeArgs.size(), executeArgsC, cache, types, 0);
			cout << "after execute" << endl;

			char* test_fname = executeArgsC[17];
			char* auxstr = strsep(&test_fname,":");
			char* test_id = strsep(&test_fname,":");
            auxstr = strsep(&test_fname,":");
            auxstr = strsep(&test_fname,":");

			//serializeData(test_id, test_fname, cache, types);			

			csOut->unregisterThread();
			csErr->unregisterThread();
			cout << "closing output files" << endl;
			jobOut->close();
			jobErr->close();

			cout << "before writing result" << endl;
			ostringstream out_ss;
			out_ss << END_TASK_TAG << " " << commandArgs[1] << " " << ret << endl;
			output = out_ss.str();


			outFile.open(outPipe);
			cout << "before result output" << endl;
                        outFile << output;
			cout << "after result output" << endl;
			fflush(NULL);
			outFile.close();
	
			cout << "end of the function" << endl;
			

		}
    }

	cout << "[Persistent C] Thread " << gettid() << " quitting with output " << output << endl;

#ifdef OMPSS_ENABLED
    cout << "disabling" << endl;
    nanos_leave_team();
    nanos_expel_current_thread();
    cout << "disabled" << endl;
#endif

	pthread_mutex_lock(&mtx);
	endedThreads++;

        cout << "TOTAL THREADS: " << nThreads << endl;
        cout << "ENDED THREADS: " << endedThreads << endl;

	if (endedThreads == nThreads){
		ostringstream out_ss;
        out_ss << QUIT_TAG << endl;
        output = out_ss.str();
		outFile.open(inDataPipe);
		outFile << output;
	}
	pthread_mutex_unlock(&mtx);

	return 0;

}






int main(int argc, char **argv) {
    if (argc < 6) {
        printf("ERROR: Incorrect number of COMPSs internal parameters\n");
        printf("Aborting...\n");
        return -1;
    }
    //Data pipes
    char* inDataPipe = argv[1];
    char* outDataPipe = argv[2];
    //Reading in pipes
    printf("numInPipes %s.\n", argv[3]);
    int numInPipes=atoi(argv[3]);
    printf("Detected %d in pipes.\n", numInPipes);
    char* inPipes[numInPipes];
    for (int i=0; i<numInPipes; i++){
        inPipes[i] = argv[i+4];
        printf("In pipe %d: %s\n",i, inPipes[i]);
    }
    //Reading out pipes
    printf("numInPipes %s.\n", argv[numInPipes+4]);
    int numOutPipes=atoi(argv[numInPipes+4]);
    printf("Detected %d out pipes.\n", numOutPipes);
    char* outPipes[numOutPipes];
    for (int i=0; i<numOutPipes; i++){
        outPipes[i] = argv[i+numInPipes+5];
	printf("Out pipe %d: %s\n",i, outPipes[i]);
    }

    fflush(NULL);

    pthread_mutex_init(&mtx,NULL);

	endedThreads = 0;

	streambuf* outbuf = cout.rdbuf();
	streambuf* errbuf = cerr.rdbuf();

    customStream *csOut = new customStream(cout.rdbuf());
	customStream *csErr = new customStream(cerr.rdbuf());

    cout.rdbuf(csOut);
	cerr.rdbuf(csErr);

	pthread_t threadpool[numInPipes];
	arg_t arguments[numInPipes];

	cout << "number of input pipes is " << numInPipes << endl;

	for (int i = 0; i < numInPipes; i++){
		arguments[i].inPipe = inPipes[i];
		arguments[i].outPipe = outPipes[i];
		arguments[i].inDataPipe = inDataPipe;
		arguments[i].outDataPipe = outDataPipe;
		arguments[i].csOut = csOut;
		arguments[i].csErr = csErr;
		arguments[i].nThreads = numInPipes;
		if(pthread_create(&threadpool[i], NULL, runThread, &arguments[i])){
			fprintf(stderr, "Error creating thread\n");
			return 1;
		}
	}


	string cmd;
	
	while (true){
		cout << "expecting to read data command" << endl;

		cmd = readline(inDataPipe);

		cout << "starting iteration" << endl;

		if (cmd != ""){

			if (cmd == QUIT_TAG) {
    	        cout << "[Persistent C] Quit received" << endl;
        	    break;
        	}

			string aux;
        	stringstream ss(cmd);

        	vector<string> dataArgs;

        	while (ss >> aux){
            	dataArgs.push_back(aux);
        	}

			if (dataArgs[0] == REMOVE_TAG){
				removeData(dataArgs[1], cache, types);	
			}

			if (dataArgs[0] == SERIALIZE_TAG){
				cout << "before serialize" << endl;
				serializeData(dataArgs[1], dataArgs[2].c_str(), cache, types);
				cout << "after serialize" << endl;

				ofstream outFile;
				ostringstream out_ss;
            	out_ss << true << endl;
            	string output = out_ss.str();

            	outFile.open(outDataPipe);
            	cout << "before result output" << endl;
                outFile << output;
            	cout << "after result output" << endl;
            	fflush(NULL);
            	outFile.close();

			}


		}

		cout << "ending iteration" << endl;

	}

	for (int i = 0; i < numInPipes; i++){
		if(pthread_join(threadpool[i], NULL)){
			fprintf(stderr, "Error joining thread\n");
			return 1;  
		}
	}
	
	cout.rdbuf(outbuf);
	cerr.rdbuf(errbuf);

	//nanos_expel_current_thread();

    cout << "[Persistent C] Worker shutting down" << endl;
   
    return 0;
} 
