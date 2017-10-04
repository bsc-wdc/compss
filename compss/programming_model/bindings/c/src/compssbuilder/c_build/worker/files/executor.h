#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <fstream>
#include <map>
#include <vector>
#include <sstream>
#include <pthread.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <iostream>
#include <customStream.h>
#include <fcntl.h>

using namespace std;
 
int execute(int argc, char **argv,  std::map<std::string, void*> &objectStorage, std::map<std::string, int> &types, int serializeOuts);

void removeData(string id, std::map<std::string, void*> &objectStorage, std::map<std::string, int> &types);

int serializeData(string id, const char* filename, std::map<std::string, void*> &objectStorage, std::map<std::string, int> &types); 

