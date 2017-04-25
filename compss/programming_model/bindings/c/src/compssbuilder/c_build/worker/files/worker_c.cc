 
#include <stdio.h>
#include "executor.h"

int main(int argc, char **argv) {
    int out = execute(argc, argv);
    if (out == 0){
        printf("Task executed successfully");
    }else{
        printf("Error task execution at worker returned %d" , out);
    }
    return out;
}
