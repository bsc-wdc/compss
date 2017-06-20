#include "executor.h"

std::map<std::string, void*> cache;

int main(int argc, char **argv) {
    int out = execute(argc, argv, cache);
    if (out == 0){
        printf("Task executed successfully");
    }else{
        printf("Error task execution at worker returned %d" , out);
    }
    return out;
}
