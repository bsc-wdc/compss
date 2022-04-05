import sys
import os
import time
from PrvLine import PrvLine
from PrvHeader import PrvHeader

CE_EVENT="8000000"

agentTraceFolders = ""
mergedFolder = ""

def getThreadId(headers, mergedHeader):
    return

def verifyCe():
    return

def verifyThreadId():
    return

#Checks the consistency between the merged header and the headers of the agents
def checkPrvHeader(agentReaders, mergedReader):

    prvHeadersLits = []
    mergedHeader = mergedReader.readline().rstrip()

    totalAppNum = 0
    totalNodeNum = 0
    apps = []
    for reader in agentReaders:
        header = reader.readline().rstrip()
        prvHead = PrvHeader(header)
        totalAppNum += prvHead.getAppNum()
        totalNodeNum += prvHead.getNodeNum()
        apps.extend(prvHead.getAppsList())


    mergedPrvHeader = PrvHeader(mergedHeader)
    if mergedPrvHeader.getAppNum() != totalAppNum:
        raise Exception("Different number of apps of on the trace header than expected")

    if mergedPrvHeader.getNodeNum() != totalNodeNum:
        raise Exception("Different number of nodes of on the trace header than expected")

    if mergedPrvHeader.getAppsList() != apps:
        raise Exception("Different aplications on the trace header than expected")

def parsePcf(file):
    result = {}
    reader = open(file, "r")
    line = reader.readline()
    while(line):
        if line == "EVENT_TYPE\n":
            line = reader.readline()
            if line == "0    "+ CE_EVENT +"    Task\n":
                line = reader.readline()
                line = reader.readline()
                while(line != "\n"):
                    values = line.split("      ")
                    result[values[0]] = values[1].rstrip()
                    line = reader.readline()
                return result
        line = reader.readline()
    raise Exception(".pcf file malformed, core elements not found")


def checkPrvBody(agentReaders, mergedReader):
    lastTimestamp = 0
    lastLineType = 0
    coreElementsPerAgent = [ [] for _ in range(len(agentReaders)) ] #list of CE events for each agent in their individual traces
    mergedCoreElementsPerAgent = [ [] for _ in range(len(agentReaders)) ] #list of CE events for each agent in the merged trace
    mer = []

    # Getting coreElementsPerAgent
    # There is one line we don't add to readLinesIndividualTraces (before entering) but there is also a line we read being None before realizing it's None
    

    readLinesIndividualTraces = 0
    for i in range(0,len(agentReaders)):
        line = agentReaders[i].readline().rstrip()
        while(line):
            prvLine = PrvLine(line)
            events = prvLine.getEvents()
            if CE_EVENT in events:
                coreElementsPerAgent[i].append(events.get(CE_EVENT))
            line = agentReaders[i].readline().rstrip()
            readLinesIndividualTraces = readLinesIndividualTraces+1

    # Getting mergedCoreElementsPerAgent and checking the order of the merged events
    line = mergedReader.readline().rstrip()
    # We have read one line but at the last iteration we will also read a None line and incrementing the counter before realizing
    readLinesMergedTraces = 0
    while(line):
        prvLine = PrvLine(line)
        thisTimestamp = prvLine.getTimeStamp()
        thisLineType = prvLine.getLineType()
        if int(lastTimestamp) > int(thisTimestamp):
            raise Exception(".prv lines are not correctly ordered, found " + thisTimestamp +  " after " + lastTimestamp)
        lastTimestamp = thisTimestamp
        lastLineType = thisLineType
        events = prvLine.getEvents()
        machineId = int(prvLine.getMachineId())-1
        if CE_EVENT in events:
            mergedCoreElementsPerAgent[machineId].append(events.get(CE_EVENT))
        line = mergedReader.readline().rstrip()
        readLinesMergedTraces = readLinesMergedTraces + 1
    print("merged events are correctly ordered")

    # Checking if the number of lines of the merged fire is correct
    if readLinesMergedTraces != readLinesIndividualTraces:
        raise Exception("Number of lines in the merged .prv file is not equal to the sum of the individual merged traces")
    print("The number of lines on the merged file is correct")

    # Translating the core elements of the individual agent traces by the values of their individual .pcf
    mergedPcfDic = parsePcf(getPcf(mergedFolder))

    for i in range(0, len(coreElementsPerAgent)):
        agentPcfDic = parsePcf(getPcf(agentTraceFolders[i]))
        for j in range(0, len(coreElementsPerAgent[i])):
            originalTag = agentPcfDic[coreElementsPerAgent[i][j]]
            mergedTag = mergedPcfDic[mergedCoreElementsPerAgent[i][j]]
            if originalTag != mergedTag:
                raise Exception("CE names differ in the original trace and in the merged trace: expected " + originalTag +  " got " + mergedTag)
    print("The core elements are properly merged and translated")


def getPrv(folder):
    for file in os.listdir(folder):
        if file.endswith(".prv"):
            return folder+"/"+file
    raise Exception("Prv file not found at: " + folder)

def getPcf(folder):
    for file in os.listdir(folder):
        if file.endswith(".pcf"):
            return folder+"/"+file
    raise Exception("Prv file not found at: " + folder)

def getRow(folder):
    for file in os.listdir(folder):
        if file.endswith('.row'):
            return folder+"/"+file
    raise Exception("Row file not found at: " + folder)

def checkPrvFiles(agentTraceFolders, mergedFolder):
    agentReaders = []
    mergedReader = open(getPrv(mergedFolder), "r")
    for folder in agentTraceFolders:
        agentReaders.append(open(getPrv(folder), "r"))

    #checkPrvHeader(agentReaders, mergedReader)
    for agentReader in agentReaders:
        agentReader.readline()
    mergedReader.readline()
    checkPrvBody(agentReaders, mergedReader)

def checkRowFiles(agentTraceFolders, mergedFolder):
    return

def main():
    print("")
    print("#####################################")
    print("######  CHECKING MERGE RESULT  ######")
    print("#####################################")
    global agentTraceFolders
    global mergedFolder
    agentTraceFolders = sys.argv[1:5]
    mergedFolder = sys.argv[5]
    checkPrvFiles(agentTraceFolders, mergedFolder)
    checkRowFiles(agentTraceFolders, mergedFolder)
    

if __name__ == "__main__":
    main()
