#include <iostream>
#include <string>

using namespace std;

int main(int argc, char **argv) {  
    // Check arguments length
    if (argc != 2) {
      cerr << "[ERROR] Incorrect number of parameters" << endl;
      return 1;
    }

    // Get argument message
    string msg = argv[1];
    cout << msg << endl;

    // Print something in the STD ERR
    cerr << "Can you read this" << endl;

    //-------------------------------------------
    // FINISH
    return 0;
}

