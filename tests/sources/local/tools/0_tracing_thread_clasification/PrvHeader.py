import re


class PrvHeader:

    SPLIT_HEADER_REGEX = ":\\s*(?![^()]*\\))"

    @staticmethod
    def __getInsideParenthesis(s):
        return s[s.find("(")+1:s.find(")")]

    @staticmethod
    def __getBeforeParenthesis(s):
        return s[0:s.find("(")]

    def __init__(self,header):
        headerParts = re.split(self.SPLIT_HEADER_REGEX,header)
        if header.endswith(",0"):
            header = header[0:len(header) - 2]

        if len(headerParts) < 5:
            raise Exception("prv header doesn't have the expected format: " + header)

        self.__singAndDate = headerParts[0]
        self.__duration = headerParts[1]
        self.__nodesNum =  PrvHeader.__getBeforeParenthesis(headerParts[2])
        self.__cpusPerNode = PrvHeader.__getInsideParenthesis(headerParts[2]).split(",")
        self.__appNum = headerParts[3]
        self.__applicationList = []
        for i in range(4,len(headerParts)):
            self.__applicationList.append(headerParts[i])

    def getAppNum(self):
        return int(self.__appNum)


    def getCpusPerNode(self):
        return self.__cpusPerNode

    def getNodeNum(self):
        return int(self.__nodesNum)

    def getAppsList(self):
        return self.__applicationList

    # Returns the number of tasks of an app from the appList
    def getNumTasksApp(self, appNum):
        return self.__getBeforeParenthesis(self.__applicationList[appNum])

    # Returns a list with the number of threads per each node on the specified app
    def getNThreadsPerApp(self, appNum):
        result = []
        threadsNode = PrvHeader.__getInsideParenthesis(self.__applicationList[appNum])
        values = threadsNode.split(",")
        for val in values:
            result.append(val.split(":")[0])
        return result
