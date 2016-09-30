#include <vector>
#include <string>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <map>

using namespace std;
  
//map that contains the elapsedTimesMeans
//[numTasks][taskDeepness][taskSleepTime][numWorkers][i] = timeElapsed_i
map<int, map<int, map<int, map<int, vector<float> > > > > elapsedTimes;

class Execution
{
public:
    int numTasks, taskDeepness, taskSleepTime, numWorkers;
    double elapsedTime;
    string fileName;
    
    Execution() 
    {
        numTasks = taskDeepness = taskSleepTime = numWorkers = -1;
        elapsedTime = -1.0f;
        fileName = "unknown";
    }
    
    void ProcessLine(string &line)
    {
        if(line.find("{{") != string::npos)
        {
            int l = line.find("{{") + 2;
            int r = line.find("}}");
            if(l != string::npos && r != string::npos)
            {
                string numStr = line.substr(l, (r-l));
                if(numTasks < 0)            numTasks        = atoi( numStr.c_str() );
                else if(taskDeepness < 0)   taskDeepness    = atoi( numStr.c_str() );
                else if(taskSleepTime < 0)  taskSleepTime   = atoi( numStr.c_str() );
                else if(elapsedTime < 0)     elapsedTime     = atof( numStr.c_str() );
            }
        }
        
        if(line.find("#BSUB -n ") != string::npos)
        {
            int l = line.find("#BSUB -n ") + 9;
            int r = line.size() - l;
            string numStr = line.substr(l, (r-l));
            numWorkers = atoi( numStr.c_str() ) - 1;
        }
        
        if(numTasks > 0 && taskDeepness > 0 && taskSleepTime > 0 && elapsedTime > 0)
        {
            elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers].push_back(elapsedTime);
        }
    }
    
    float getElapsedTimeMeans()
    {
        float min = elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][0];
        for(int i = 1; i < elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers].size(); ++i)
        {
            if(elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][i] < min)
                min = elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][i];
        }
        return min;
        
        /*
        float sum = 0.0f;
        for(int i = 0; i < elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers].size(); ++i)
        {
            sum += elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][i];
        }
        return sum / elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers].size();
        */
    }
    
    void printRowHeader()
    {
        cout <<  "nt:"  << numTasks      << " " << 
                 "deep:"  << taskDeepness  << " " << 
                 "tst:" << taskSleepTime << " " << 
                 "nw:"  << numWorkers    << " ";
    }
    
    string toString()
    {
        string SEP = ", ";
        string out = "";
        string eTime = (elapsedTime < 0) ? "********* ERROR *********" : "elapsedTime: " + to_string(elapsedTime);
        out = "[nt:" + to_string(numTasks) + SEP + 
                "deep:" + to_string(taskDeepness) + SEP + 
                "sleepTime:" + to_string(taskSleepTime) + SEP + 
                eTime + SEP +
                "idealTime:" + to_string(getIdealTime()) + SEP +
                "nw:" + to_string(numWorkers) + "]" + SEP +
                "File:\"" + fileName + "\"";
        return out;
    }
    
    int getIdealTime() 
    {
        return ((numTasks/16) * taskSleepTime) / numWorkers;
    }
    
    bool equals(const Execution &other)
    {
        return numTasks == other.numTasks && 
               taskDeepness == other.taskDeepness && 
               taskSleepTime == other.taskSleepTime &&
               numWorkers == other.numWorkers;
    }
    
    inline bool operator() (const Execution& e1, const Execution& e2)
    {
        if (e1.elapsedTime < 0) return false;
        if (e2.elapsedTime < 0) return true;
	
	if (e1.numWorkers < e2.numWorkers) return true;
	if (e1.numWorkers > e2.numWorkers) return false;
	if (e1.numWorkers == e2.numWorkers)
	{
	    if (e1.numTasks < e2.numTasks) return true;
	    if (e1.numTasks > e2.numTasks) return false;
	    if (e1.numTasks == e2.numTasks)
	    {
		if (e1.taskSleepTime < e2.taskSleepTime) return true;
		if (e1.taskSleepTime > e2.taskSleepTime) return false;
		if (e1.taskSleepTime == e2.taskSleepTime) return false;
	    }
	}
	return false;
    }
};

int main(int argc, char** args)
{
    //Get parameters 
    if(argc < 3 || strlen(args[1]) != 1 || (args[1][0] != 'v' && args[1][0] != '-')) 
    {
        cout << "Usage: formatOut.exe verbose('-' or 'v')  file1 [file2 [...] ]" << endl;
        exit(0);
    }
    
    bool verbose = (args[1][0] == 'v');
    //
    
    
    //Read files and fill vector of Executions //////////////////////////////////
    vector<Execution> executions;
    bool numTasksFound, tasksDeepnessFound, taskSleepTimeFound, elapsedTimesFound;
    for(int i = 3; i < argc; ++i)
    {
        ifstream ifs;
        ifs.open (args[i], ifstream::in | ios_base::binary);
        if (ifs.is_open())
        {
            executions.push_back(Execution()); //new Execution
            executions[executions.size()-1].fileName = args[i];
            string line;
            while (getline (ifs, line)) 
                executions[executions.size()-1].ProcessLine(line); //line by line
        }
        ifs.close();
    }
    /////////////////////////////////////////////////////////////////////////////
    
    //Sort the executions
    sort(executions.begin(), executions.end(), Execution());
    
    //If verbose, print detailed information
    if(verbose)
    {
        cout << "Executions info: \t" << endl;
        for(int i = 1; i <= executions.size(); ++i)  cout << i << ":   \t " << executions[i-1].toString() << endl;
        cout << endl << endl;
        cout << "::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::" << endl;
    }
    ////////////////////////////////////////////
    
    //some info message of the top row being used
    cout << endl;
    cout << "#Row format:  numTasks taskDeepness taskSleepTime numWorkers   elapsedTime0 elapsedTime1 elapsedTime2 ... elapsedTimeN" << endl;
    //
    
    //Print the table //////////////////////////////////////
    int i = 0, last = i;
    bool firstIter = true;
    executions[i].printRowHeader();
    for(int i = 0; i < executions.size(); ++i)
    {
        if (executions[i].numTasks < 0 || executions[i].numWorkers < 0) //ERROR
        {
            last=i; continue; //ERROR in this dataset
        }
        
        if((!firstIter && executions[i].equals(executions[last])))
        {
            continue;
        }
        else //not a top row repeated value, must print
        {
            if ( 
		  ( 
		    (executions[i].numTasks > executions[last].numTasks)  
		    //|| ( workersAsTopRow && executions[i].taskSleepTime > executions[last].taskSleepTime) 
		    || (executions[i].numWorkers > executions[last].numWorkers)
		  ) 
		&& 
		executions[i].getElapsedTimeMeans() > 0
	       )
            {
                //New row
                cout << endl;                
		executions[i].printRowHeader();
            }
            cout << ((executions[i].elapsedTime > 0) ? to_string(executions[i].getElapsedTimeMeans()) : "?") << " ";
            firstIter = false;
            last = i;
        }
    }
    ////////////////////////////////////////////////////////////
        
    return 0;
}
