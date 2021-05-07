class PrvLine:
    # Positions in .prv when spliting the line on ':'
    __TIMESTAMP_POS = 5
    # STATE_ variables apply to lines describing an state change prv event
    __STATE_MACHINE_POS = 2
    __STATE_RUNTIME_EXECUTOR_POS = 3
    __STATE_THREAD_NUMBER_POS = 4
    __STATE_EVENTS_START_POS = 6
    # COM_ variables apply to lines describing a comunication prv event
    __COM_SEND_MACHINE_POS = 2
    __COM_SEND_RUNTIME_EXECUTOR_POS = 3
    __COM_SEND_THREAD_NUMBER_POS = 4
    __COM_RECIV_MACHINE_POS = 8
    __COM_RECIV_RUNTIME_EXECUTOR_POS = 9
    __COM_RECIV_THREAD_NUMBER_POS = 1

    __values=[]

    def __init__(self, line):
        self.__values = line.split(":")

    def getTimeStamp(self):
        return self.__values[self.__TIMESTAMP_POS]

    def getMachineId(self):
        return self.__values[self.__STATE_MACHINE_POS]

    def getFullThreadId(self):
        return self.__values[self.__STATE_MACHINE_POS] +":"+ self.__values[self.__STATE_RUNTIME_EXECUTOR_POS] +":"+ self.__values[self.__STATE_THREAD_NUMBER_POS]

    def getEvents(self):
        result = {}
        if self.__values[0] == "3":
            return result
        for i in range(6, len(self.__values), 2):
            result[self.__values[i]] = self.__values[i+1]
        return result

    def getLineType(self):
        return self.__values[0]
