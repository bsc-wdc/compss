/*
 *  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <vector>

#define DEBUG_BINDING

#include "Matmul.h"
#include "Matrix.h"
#include "Block.h"
#include <unistd.h>
#include <sys/time.h>


int N;  //MSIZE
int M;	//BSIZE
double val;

void usage() {
    cerr << "[ERROR] Bad number of parameters" << endl;
    cout << "    Usage: Matmul <N> <M> <val>" << endl;
}

int main(int argc, char **argv) {
	
	struct timeval t_init_start, t_init_end, t_comp_start, t_comp_end;

	if (argc != 4) {
		usage();
		return -1;
	} else {
		N = atoi(argv[1]);
		M = atoi(argv[2]);
		val = atof(argv[3]);

		compss_on();

		cout << "Running with the following parameters:\n";
		cout << " - N: " << N << "\n";
		cout << " - M: " << M << "\n";
		cout << " - val: " << val << "\n";

		gettimeofday(&t_init_start, NULL);


		Matrix A(N);	
		A.init(N,M,val);
		Matrix B(N);
		B.init(N,M,val);
		Matrix C(N);
		C.init(N,M,0.0);


		for (int i=0; i<N; i++) {
          	      for (int j=0; j<N; j++) {
                    	initBlock(A.data[i][j], M, val);
			initBlock(B.data[i][j], M, val);
			initBlock(C.data[i][j], M, 0.0);
                      }
         	}

		gettimeofday(&t_init_end, NULL);
		gettimeofday(&t_comp_start, NULL);

		waitForAllTasks();		

		for (int i=0; i<N; i++) {
                	for (int j=0; j<N; j++) {
                        	for (int k=0; k<N; k++) {
					multiplyBlocks(C.data[i][j], A.data[i][k], B.data[k][j]);
				}

                	}
        	}

        	for (int i=0; i<N; i++) {
                	for (int j=0; j<N; j++) {

                        	compss_wait_on(*C.data[i][j]);

                	}
        	}

		compss_off();

		gettimeofday(&t_comp_end, NULL);
		
		cout << "The result is ";
                C.result();
                cout << endl;

		double init_msecs, comp_msecs, total_msecs;

                init_msecs = (((t_init_end.tv_sec - t_init_start.tv_sec) * 1000000) + (t_init_end.tv_usec - t_init_start.tv_usec))/1000;
                comp_msecs = (((t_comp_end.tv_sec - t_comp_start.tv_sec) * 1000000) + (t_comp_end.tv_usec - t_comp_start.tv_usec))/1000;
                total_msecs = (((t_comp_end.tv_sec - t_init_start.tv_sec) * 1000000) + (t_comp_end.tv_usec - t_init_start.tv_usec))/1000;

		cout << "Results for execution with CPU, N: " << N << ", M: " << M << endl;
                cout << "Initialization time: " << init_msecs << " ms\n";
                cout << "Computation time: " << comp_msecs << " ms\n";
                cout << "Total time: " << total_msecs << "ms\n";

	}

	return 0;
}
