import sys
import os
import time
from PrvLine import PrvLine
from PrvHeader import PrvHeader

TH_IDENT_EVENT="8001003"


runcompssThreads = {}
agentThreads = {}
runcompssHeader = ""
agentHeader = ""
runcompssThIdentEvents = {}
agentThIdentEvents = {}

def parsePrvs(runcompssPrv, agentPrv):
    global runcompssThreads
    global agentThreads
    global runcompssHeader
    global agentHeader
    global runcompssThIdentEvents
    global agentThIdentEvents
    
    reader = open(runcompssPrv, "r")
    runcompssHeader = reader.readline().rstrip()
    line = reader.readline().rstrip()
    while(line): 
        prvLine = PrvLine(line)
        events = prvLine.getEvents()
        threadId = prvLine.getFullThreadId()
        if threadId not in runcompssThreads:
            runcompssThreads[threadId] = None
        if TH_IDENT_EVENT in events:
            identifierEvent = events.get(TH_IDENT_EVENT)
            if threadId == "1.1.1":
                raise Exception("Main thread has an identification event, but it shouldn't")
            if threadId in runcompssThIdentEvents and identifierEvent != "0":
                raise Exception("Thread " + threadId + " has more than one identification event")
            if threadId not in runcompssThIdentEvents: 
                runcompssThIdentEvents[threadId] = identifierEvent
        line = reader.readline().rstrip()  

    reader = open(agentPrv, "r")
    agentHeader = reader.readline().rstrip()
    line = reader.readline().rstrip()
    while(line):
        prvLine = PrvLine(line)
        events = prvLine.getEvents()
        threadId = prvLine.getFullThreadId()
        if threadId not in agentThreads:
            agentThreads[threadId] = None
        if TH_IDENT_EVENT in events and threadId not in agentThIdentEvents:
            agentThIdentEvents[threadId] = events.get(TH_IDENT_EVENT)
        line = reader.readline().rstrip()

def checkHeader(header, threads):
    threadsPerTaskPerApp = [[]]  #list of number of threads per each node of each app
    for thId in threads:
        values = thId.split(":")
        # -1 to app and node because we use it as a position for the array and the threads number start from 1
        app = int(values[0])-1
        task = int(values[1])-1
        thread = int(values[2])
        while app >= len(threadsPerTaskPerApp):
            threadsPerTaskPerApp.append([])
        while task >= len(threadsPerTaskPerApp[app]):
            threadsPerTaskPerApp[app].append(0)
        if threadsPerTaskPerApp[app][task] < thread:
            threadsPerTaskPerApp[app][task] = thread


    prvHeader = PrvHeader(header)
    ## Commented until infrastructure is properly managed in the traces
    # if prvHeader.getNodeNum() != len(threadsPerTaskPerApp):
    #    raise Exception("Malformed .prv header, threre's threads from  " + str(len(threadsPerTaskPerApp)) + " nodes but the header indicates " + str(prvHeader.getNodeNum()) + " nodes.")
    # cpusPerNode = prvHeader.getCpusPerNode()
    # for i in range(0,len(cpusPerNode)):
    #    cpusInThisNode = 0
    #    for j in range(0,len(threadsPerTaskPerApp[i])):
    #        cpusInThisNode = cpusInThisNode + threadsPerTaskPerApp[i][j]
    #    if int(cpusPerNode[i]) != cpusInThisNode:
    #        raise Exception("Malformed .prv header, there seems to be  " + str(cpusInThisNode) + " cpus in node " + str(i) + " but header indicates " + cpusPerNode[i])

    if prvHeader.getAppNum() != len(threadsPerTaskPerApp):
        raise Exception("Malformed .prv header, threre's threads from  " + str(len(threadsPerTaskPerApp)) + " apps but the header indicates " + str(prvHeader.getAppNum()) + " apps")    
    appList = prvHeader.getAppsList()
    if len(appList) != len(threadsPerTaskPerApp):
        raise Exception("Malformed .prv header, application list has  " + str(len(appList)) + " elements. Expected " + str(len(threadsPerTaskPerApp)))

    for i in range(0,len(appList)):
        if int(prvHeader.getNumTasksApp(i)) != len(threadsPerTaskPerApp[i]):
            raise Exception("Malformed .prv header, application number  " + str(i+1) + " has "+ str(prvHeader.getNumTasksApp(i)) + " tasks. Expected " + str(len(threadsPerTaskPerApp[i])))
        numThreadsPerApp = prvHeader.getNThreadsPerApp(i)
        for j in range(0,len(numThreadsPerApp)):
            if int(numThreadsPerApp[j]) != threadsPerTaskPerApp[i][j]:
                raise Exception("Malformed .prv header, application number  " + str(i+1) + " has "+ numThreadsPerApp[j] + " tasks  in the " + str(j+1) + " node. Expected " + str(threadsPerTaskPerApp[i][j]))



def getPrv(folder):
    for file in os.listdir(folder):
        if file.endswith(".prv"):
            return folder+"/"+file
    raise Exception("Prv file not found at: " + folder)

def getRow(folder):
    for file in os.listdir(folder):
        if file.endswith('.row'):
            return folder+"/"+file
    raise Exception("Row file not found at: " + folder)

def checkThreadOrder(thA, thB):
    valuesA = thA.split(":")
    valuesB = thB.split(":")
    for i in range(0,2):
        if valuesA > valuesB:
            raise Exception("Unexpected thread order in .row file: " + thA + " before " + thB)
        if valuesA < valuesB:
            return

def checkThreadTranslations(threads, thIdentifiers, rowFile):
    reader = open(rowFile, "r")
    line = reader.readline()
    while not line.startswith("LEVEL THREAD SIZE"):
        line = reader.readline().rstrip()
    
    line = reader.readline().rstrip()
    if not line:
        raise Exception("Malformed .row file, LEVEL THREAD SIZE not found")


    lastThreadId = "1:1:1"
    while line:
        if not line.startswith("THREAD "):
            tag = line[0:line.find("(")-1] #-1 cause of the empty space between the tag and the (X:Y:Z)
            thId = line[line.find("(")+1:line.find(")")].replace(".",":")
            checkThreadOrder(lastThreadId, thId)
            expectedTag = ""
            if thId == "1:1:1":
                expectedTag = "MAIN APP"
            elif thId.endswith(":1:1"):
                expectedTag = "WORKER MAIN"
            elif thId in thIdentifiers:
                identifierEvent = thIdentifiers[thId]
                if identifierEvent == "2":
                    expectedTag = "RUNTIME AP"
                if identifierEvent == "3":
                    expectedTag = "RUNTIME TD"
                if identifierEvent == "4":
                    expectedTag = "RUNTIME FS L"
                if identifierEvent == "5":
                    expectedTag = "RUNTIME FS H"
                if identifierEvent == "6":
                    expectedTag = "RUNTIME TIMER"
                if identifierEvent == "7":
                    expectedTag = "RUNTIME WALLCLOCK"
                if identifierEvent == "8":
                    expectedTag = "EXECUTOR"
            if expectedTag == "":
                raise Exception("Unknown thread in .row file for thread identification event " + identifierEvent)
            if tag != expectedTag:
                raise Exception("Unexpected tag in .row file. Expected " + expectedTag + " got " + tag)

            if thId in threads:
                threads[thId] = 1
            for th in threads:
                if th is None:
                    raise Exception("The thead " + th + " is not present in the .row file")
            lastThreadId = thId
        line = reader.readline().rstrip()



def main():
    runcompssDir = sys.argv[1]
    agentDir = sys.argv[2]
    runcompssPrv = getPrv(runcompssDir)
    agentPrv = getPrv(agentDir)

    parsePrvs(runcompssPrv, agentPrv)

    print("")
    print("#####################################")
    print("#####  CHECKING RUNCOMPSS TRACE  ####")
    print("#####################################")
    # checkHeader(runcompssHeader, runcompssThreads)
    checkThreadTranslations(runcompssThreads, runcompssThIdentEvents, getRow(runcompssDir))

    print("")
    print("#####################################")
    print("######  CHECKING AGENT TRACE  #######")
    print("#####################################")
    # checkHeader(agentHeader, agentThreads)
    checkThreadTranslations(agentThreads, agentThIdentEvents, getRow(agentDir))

if __name__ == "__main__":
    main()
