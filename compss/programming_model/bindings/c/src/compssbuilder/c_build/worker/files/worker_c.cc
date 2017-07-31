#include "executor.h"

std::map<std::string, void*> cache;
std::map<std::string, int> types;

int main(int argc, char **argv) {
    int out = execute(argc, argv, cache, types, 1);
    if (out == 0){
        printf("Task executed successfully");
    }else{
        printf("Error task execution at worker returned %d" , out);
    }
    return out;
}
