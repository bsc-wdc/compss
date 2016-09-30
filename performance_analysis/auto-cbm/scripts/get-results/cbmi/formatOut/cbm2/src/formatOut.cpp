#include <vector>
#include <string>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <map>

using namespace std;

//map that contains the elapsedTimes
//[numTasks][taskDeepness][taskSleepTime][numWorkers][txSize][inout][files][i] = timeElapsed_i
map<int, map<int, map<int, map<int, map<int, map<int, map<int, vector<float> > > > > > > > elapsedTimes;

class Execution
{
public:
    int numTasks, taskDeepness, taskSleepTime, numWorkers, txSize;
    double elapsedTime;
    int inout=-1, files=-1; //inout (-1=null, 0=in, 1=inout),   files (-1=null, 0=objects, 1=files) 
    string fileName;
    
    Execution() 
    {
        numTasks = taskDeepness = taskSleepTime = numWorkers = txSize = -1;
        inout = files = -1;
        elapsedTime = -1.0f;
        fileName = "unknown";
    }
    
    void ProcessLine(string &line)
    {
        if(line.find("{{") != string::npos)
        {
            int l = line.find("{{") + 2, r = line.find("}}");
            if(l != string::npos && r != string::npos)
            {
                string content = line.substr(l, (r-l));
                if(numTasks < 0)            numTasks        = atoi( content.c_str() );
                else if(taskDeepness < 0)   taskDeepness    = atoi( content.c_str() );
                else if(taskSleepTime < 0)  taskSleepTime   = atoi( content.c_str() );
                else if(txSize < 0) txSize = atoi( content.c_str() );
                else if(inout == -1) inout = (content == "INOUT") ? 1 : 0;
                else if(files == -1) files = (content == "FILES") ? 1 : 0;
                else if(elapsedTime < 0)    elapsedTime     = atof( content.c_str() );
            }
            
            l = line.find("[{") + 2;
            r = line.find("}]");
            if(l != string::npos && r != string::npos)
            {
                string content = line.substr(l, (r-l));
            }
        }
        
        if(line.find("#BSUB -n ") != string::npos)
        {
            int l = line.find("#BSUB -n ") + 9;
            int r = line.size() - l;
            string content = line.substr(l, (r-l));
            numWorkers = atoi( content.c_str() ) - 1;
        }
        
        if(numTasks > 0 && taskDeepness > 0 && taskSleepTime >= 0 && elapsedTime > 0 && txSize > 0 && inout >= 0 && files >= 0)
        {
            elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][txSize][inout][files].push_back(elapsedTime);
        }
    }
    
    float getElapsedTimeMeans()
    {
        float min = elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][txSize][inout][files][0];
        for(int i = 1; i < elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][txSize][inout][files].size(); ++i)
        {
            if(elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][txSize][inout][files][i] < min)
                min = elapsedTimes[numTasks][taskDeepness][taskSleepTime][numWorkers][txSize][inout][files][i];
        }
        return min;
    }
    
    string getTXSizeString() 
    {
        string suff = "";
        if(txSize >= 1000000000) return to_string(txSize/1000000000) + "\tGB";
        else if(txSize >= 1000000) return to_string(txSize/1000000) + "\tMB";
        else if(txSize >= 1000) return to_string(txSize/1000) + "\tKB";
        else return to_string(txSize) + "\tB"; 
    }
    
    int getModeId() const
    {
        if(inout) { if(files) return 0; else return 1; }
        else { if(files) return 2; else return 3; }
        return 3;
    }
    
    string getModeString()
    {
        return string(inout==1 ? "IO" : "I") + string(files==1 ? "FILES" : "OBJECTS");
    }
    
    void printRowHeader()
    {
        cout <<  "nt:"  << numTasks      << " " << 
                 "deep:"  << taskDeepness  << " " << 
                 "tst:" << taskSleepTime << " " << 
                 "nw:"  << numWorkers    << " " <<
                 "tx:"  << txSize        << " " <<
                (inout==1 ? "IO" : "I")  << " " <<
                (files==1 ? "FILES" : "OBJECTS") << " ";
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
                "tx:" + getTXSizeString() + SEP +
                "mode:" + getModeString() + SEP + 
                "nw:" + to_string(numWorkers) + "]" + SEP +
                "File:\"" + fileName + "\"";
        return out;
    }
    
    int getIdealTime() 
    {
        return (int)(((float(numTasks))/(numWorkers*16)) * taskDeepness * taskSleepTime);
    }
    
    bool equals(const Execution &other)
    {
        return numTasks == other.numTasks && 
               taskDeepness == other.taskDeepness && 
               taskSleepTime == other.taskSleepTime &&
               txSize == other.txSize &&
               getModeId() == other.getModeId() &&
               numWorkers == other.numWorkers;
    }
    
    inline bool operator() (const Execution& e1, const Execution& e2)
    {
        if (e1.txSize < 0) return false;
        if (e2.txSize < 0) return true;
            
        if (e1.numTasks < e2.numTasks) return true;
        if (e1.numTasks > e2.numTasks) return false;
        if (e1.numTasks == e2.numTasks)
        {
            if (e1.taskDeepness < e2.taskDeepness) return true;
            if (e1.taskDeepness > e2.taskDeepness) return false;
            if (e1.taskDeepness == e2.taskDeepness)
            {
                if (e1.taskSleepTime < e2.taskSleepTime) return true;
                if (e1.taskSleepTime > e2.taskSleepTime) return false;
                if (e1.taskSleepTime == e2.taskSleepTime)
                {
                    if (e1.numWorkers < e2.numWorkers) return true;
                    if (e1.numWorkers > e2.numWorkers) return false;
                    if (e1.numWorkers == e2.numWorkers)
                    {
                        if (e1.txSize < e2.txSize) return true;
                        if (e1.txSize > e2.txSize) return false;
                        if (e1.txSize == e2.txSize)
                        {
                            if(e1.getModeId() < e2.getModeId()) return true;
                            if(e1.getModeId() > e2.getModeId()) return false;
                            if(e1.getModeId() == e2.getModeId()) return false;
                        }
                    }
                }
            }
        }
        return false;
    }
};

int main(int argc, char** args)
{
    //Get parameters 
    if(argc < 3 || strlen(args[1]) != 1 || (args[1][0] != 'v' && args[1][0] != '-') ) 
    {
        cout << "Usage: formatOut.exe verbose('-' or 'v') file1 [file2 [...] ]" << endl;
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
    cout << "#Mode as top row (IOFILES, IOOBJ, IFILES, IOBJ)";
    cout << endl;
    cout << "#Row format:  numTasks taskDeepness taskSleepTime numWorkers txSize (IO/I) (FILES/OBJ) elapsedTime0 elapsedTime1 elapsedTime2 ... elapsedTimeN" << endl;
    //
    
    //Print the table //////////////////////////////////////
    int i = 0, last = i;
    bool firstIter = true;
    if(executions.size() == 0) { cout << "No executions found" << endl; return -1 ; }
    executions[0].printRowHeader();
    
    for(int i = 0; i < executions.size(); ++i)
    {
        //cout << i << ": " << executions[i].toString();
        if (executions[i].numTasks < 0 || executions[i].numWorkers < 0 || executions[i].txSize < 0) //ERROR
        {
           // cout << "   ERROR -> ++last; continue;" << endl;
            last=i; continue; //ERROR in this dataset
        }
        
        if((!firstIter && executions[i].equals(executions[last])))
        {
           // cout << "   Skipping" << endl;
            continue;
        }
        else //not a top row repeated value, must print
        {
            if (executions[i].getModeId() < executions[last].getModeId())
            {
                //New row
                cout << endl;
               // cout << "  New row, ";
                executions[i].printRowHeader();
            }
            //cout << "  Print time" << endl;
            cout << ((executions[i].elapsedTime > 0) ? to_string(executions[i].getElapsedTimeMeans()) : "?") << " ";
            firstIter = false;
            last = i;
        }
    }
    ////////////////////////////////////////////////////////////
        
    return 0;
}
