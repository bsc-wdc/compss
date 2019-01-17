#include <iostream>
#include <sys/time.h>

using namespace std;


// TIME DEFINITIONS AND FUNCTIONS
#ifdef TIME
#define IF_TIME(foo) foo;
#else
#define IF_TIME(foo)
#endif

double rtclock() {
        struct timezone Tzp;
        struct timeval Tp;
        int stat = gettimeofday (&Tp, &Tzp);
        if (stat != 0) {
                cerr << "ERROR: Cannot get return from gettimeofday: " << stat << endl;
        }
        return(Tp.tv_sec + Tp.tv_usec*1.0e-6);
}


// AUXILIAR MATRIX FUNCTIONS
double** initialize(int xSize, int ySize, int bSize, bool init) {
        double** matrix = new double*[xSize];

        for (int i = 0; i < xSize; ++i) {
                matrix[i] = new double[ySize];
                for (int j = 0; j < ySize; ++j) {
                        if (init) {
                                matrix[i][j] = (double)(i + j);
                        } else {
                                matrix[i][j] = 0.0;
                        }
                }
        }

        return matrix;
}

void printMatrix(double** matrix, int xSize, int ySize) {
        for (int i = 0; i < xSize; ++i) {
                for (int j = 0; j < ySize; ++j) {
                        cout << matrix[i][j] << " ";
                }
                cout << endl;
        }
}


// MAIN COMPUTATION
void matmul(const int mSize, const int nSize, const int kSize, const int bSize, bool debug) {
        // Initialize
        double** a = initialize(mSize, nSize, bSize, true);
        double** b = initialize(nSize, kSize, bSize, true);
        double** c = initialize(mSize, kSize, bSize, false);

        // Debug
        if (debug) {
                cout << "Matrix A:" << endl;
                printMatrix(a, mSize, nSize);
                cout << "Matrix B:" << endl;
                printMatrix(b, nSize, kSize);
                cout << "Matrix C:" << endl;
                printMatrix(c, mSize, kSize);
        }

        // Perform computation
        #pragma scop
        for (int i = 0; i < mSize; ++i) {
                for (int j = 0; j < kSize; ++j) {
                        for (int k = 0; k < nSize; ++k) {
                                c[i][j] += a[i][k]*b[k][j];
                        }
                }
        }
        #pragma endscop

        // Debug
        if (debug) {
                cout << "Matrix C:" << endl;
                printMatrix(c, mSize, kSize);
        }

        // Free
        free(a);
        free(b);
        free(c);
}


// MAIN METHOD
int main() {
        // Parse arguments
        const int mSize = 5;
        const int nSize = 2;
        const int kSize = 3;
        const int bSize = 1;
        const bool debug = true;

        // Perform computation
        double startTime, endTime;
        IF_TIME(startTime = rtclock());
        matmul(mSize, nSize, kSize, bSize, debug);
        IF_TIME(endTime = rtclock());

        // Log results and time
        IF_TIME(cout << "Elapsed Time: " << (endTime - startTime) << " (s)" << endl);
}

