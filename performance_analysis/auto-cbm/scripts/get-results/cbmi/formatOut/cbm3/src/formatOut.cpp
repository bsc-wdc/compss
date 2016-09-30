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
//[deepness][taskSleepTime][numWorkers][txSize][inout][files][cbm][i] = timeElapsed_i
map<int, map<int, map<int, map<int, map<int, map<int, map<int, vector<float> > > > > > > > elapsedTimes;

class Execution
{
public:
    int deepness, taskSleepTime, numWorkers, txSize, inout, files, cbm;
    double elapsedTime;
    bool addedToMap;
    string fileName;
    
    //inout (-1=null, 0=in, 1=inout),   files (-1=null, 0=objects, 1=files) 
    Execution() 
    {
        addedToMap = false;
        deepness = taskSleepTime = numWorkers = txSize = inout = files = cbm = -1;
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
                if(deepness < 0)   deepness    = atoi( content.c_str() );
                else if(taskSleepTime < 0)  taskSleepTime   = atoi( content.c_str() );
                else if(txSize < 0) txSize = atoi( content.c_str() );
                else if(inout == -1) inout = (content == "INOUT") ? 1 : 0;
                else if(files == -1) files = (content == "FILES") ? 1 : 0;
                else if(elapsedTime < 0) elapsedTime = atof( content.c_str() );
            }
        }
        
        if(line.find("Starting cbm") != string::npos)
        {
            int l = line.find("cbm") + 3;
            string cbmNum = line.substr(l, 1);
            cbm = atoi(cbmNum.c_str());
        }
        
        if(line.find("#BSUB -n ") != string::npos)
        {
            int l = line.find("#BSUB -n ") + 9;
            int r = line.size() - l;
            string content = line.substr(l, (r-l));
            numWorkers = atoi( content.c_str() ) - 1;
        }
        
        if(!addedToMap && !error())
        {
            addedToMap = true;
            elapsedTimes[deepness][taskSleepTime][numWorkers][txSize][inout][files][cbm].push_back(elapsedTime);
        }
    }
    
    float getElapsedTimeMeans()
    {
        if(formatError()) return -1.0f;
        //aunque tenga el elapsedTime < 0, puede pillar valores de otras ejecuciones, si ninguno
        //de los valores del formato falla (en caso de que falle habra algun -1, y ni se podra acceder al map)
        //usando formatError() en vez de error(), hacemos que si la primera repeticion del experimento ha fallado,
        //pueda mirar las de más adelante, sin quedarse atascado en esta porque elapsedTime < 0, imprimir "?", y hacer skip de todas las siguientes
        
        float min = elapsedTimes[deepness][taskSleepTime][numWorkers][txSize][inout][files][cbm][0];
        for(int i = 1; i < elapsedTimes[deepness][taskSleepTime][numWorkers][txSize][inout][files][cbm].size(); ++i)
        {
            float v = elapsedTimes[deepness][taskSleepTime][numWorkers][txSize][inout][files][cbm][i]; 
            if(v < min)
                min = v;
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
        cout <<  "deep:"  << deepness      << " " <<
                 "tst:"   << taskSleepTime << " " << 
                 "nw:"    << numWorkers    << " " <<
                 "tx:"    << txSize        << " " <<
                 "cbm:"   << cbm           << " " <<
                 "- " << "- ";
    }
    
    string toString()
    {
        string SEP = ", ";
        string out = "";
        string eTime = (elapsedTime < 0) ? "********* ERROR *********" : "elapsedTime: " + to_string(elapsedTime);
        out = "[deep:"       + to_string(deepness)           + SEP +
                "tst:"       + to_string(taskSleepTime)      + SEP + 
                               eTime                         + SEP +
                "tx:"        + getTXSizeString()             + SEP +
                "cbm:"       + to_string(cbm)                + SEP +
                "mode:"      + getModeString()               + SEP + 
                "nw:"        + to_string(numWorkers) + "]" + SEP +
                "File:\""    + fileName + "\"";
        return out;
    }
    
    bool formatError()
    {
        return deepness < 0 || taskSleepTime < 0 || numWorkers < 0 || txSize < 0 || inout < 0 || files < 0 || cbm < 0 || elapsedTime < 0;
    }
    
    bool error()
    {
        return formatError() || elapsedTime < 0;
    }
    
    bool isARepetitionOfSameExperiment(const Execution &other)
    {
        return deepness == other.deepness &&
               taskSleepTime == other.taskSleepTime &&
               txSize == other.txSize &&
               getModeId() == other.getModeId() &&
               numWorkers == other.numWorkers &&
               cbm == other.cbm;
    }
    
    inline bool operator() (const Execution& e1, const Execution& e2)
    {
        if (e1.txSize < 0) return false;
        if (e2.txSize < 0) return true;
            

        if (e1.deepness < e2.deepness) return true;
        if (e1.deepness > e2.deepness) return false;
        if (e1.deepness == e2.deepness)
        {
            if (e1.taskSleepTime < e2.taskSleepTime) return true;
            if (e1.taskSleepTime > e2.taskSleepTime) return false;
            if (e1.taskSleepTime == e2.taskSleepTime)
            {
                if (e1.numWorkers < e2.numWorkers) return true;
                if (e1.numWorkers > e2.numWorkers) return false;
                if (e1.numWorkers == e2.numWorkers)
                {
                    if(e1.cbm < e2.cbm) return true;
                    if(e1.cbm > e2.cbm) return false;
                    if(e1.cbm == e2.cbm) 
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
    if(argc < 3 || strlen(args[1]) != 1 || (args[1][0] != 'v' && args[1][0] != '-')) 
    {
        cout << "Usage: formatOut.exe verbose('-' or 'v') file1 [file2 [...] ]" << endl;
        exit(0);
    }
    
    bool verbose = (args[1][0] == 'v');
    //
    
    
    //Read files and fill vector of Executions //////////////////////////////////
    vector<Execution> executions;
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
    /////////////////////////////////////////
    ////////////////////////////////////////
    
    //Sort the executions
    sort(executions.begin(), executions.end(), Execution());
    
    //If verbose, print detailed information
    if(verbose)
    {
        cout << "Executions info: \t" << endl;
        for(int i = 0; i < executions.size(); ++i)  cout << (i+1) << ":   \t " << executions[i].toString() << endl;
        cout << endl << endl;
        cout << "::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::" << endl;
    }
    ////////////////////////////////////////////
    
    //some info message of the top row being used
    cout << "#Mode as top row (IOFILES, IOOBJ, IFILES, IOBJ)";
    cout << endl;
    cout << "#Row format: deepness taskSleepTime numWorkers txSize cbm (IO/I) (FILES/OBJ) elapsedTime0 elapsedTime1 elapsedTime2 ... elapsedTimeN" << endl;
    //
    
    //Print the table //////////////////////////////////////
    int i = 0, last = i;
    bool firstIter = true;
    if(executions.size() == 0) { cout << "No executions found" << endl; return -1 ; }
    executions[0].printRowHeader();
    
    for(int i = 0; i < executions.size(); ++i)
    {
        Execution& ei = executions[i];
        Execution& el = executions[last];
        if (!ei.error()) //ERROR
        {
            //repetition of same experiment, already taken the min time in ei, so skip it
            if(!firstIter && ei.isARepetitionOfSameExperiment(el)) continue;
            
            if (ei.txSize > el.txSize || ei.cbm != el.cbm)
            {
                //New row
                cout << endl;
                ei.printRowHeader();
            }

            //Print elapsed time
            if(ei.getElapsedTimeMeans() > 0) cout << ei.getElapsedTimeMeans();
            else cout << "?";
            cout << " ";
            last = i;
            firstIter = false;
        }
        else if(ei.getElapsedTimeMeans() < 0)
        {
            if (ei.cbm < el.cbm)
            {
                //New row
                cout << endl;
                ei.printRowHeader();
            }
            if(!ei.isARepetitionOfSameExperiment(el)) cout << "? ";
            last = i;
        }
    }
    ////////////////////////////////////////////////////////////
     
    return 0;
}
