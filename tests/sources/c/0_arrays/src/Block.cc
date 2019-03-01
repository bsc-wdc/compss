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

#include "Block.h"
#include <unistd.h>
#include <fstream>
#include <iostream>

Block::Block(int bSize) {
	M = bSize;
	data.resize(M);
	for (int i=0; i<M; i++) {
		data[i].resize(M);
	}
}

static void Block::generate(Block *b, int bSize, double initVal) {
	cout << "Running create..." << endl << flush;
	b->M = bSize;
        b->data.resize(bSize);
        for (int i=0; i<bSize; i++) {
                b->data[i].resize(bSize);
        }
        for (int i=0; i<bSize; i++) {
                for (int j=0; j<bSize; j++) {
                        b->data[i][j] = initVal;
                }
        }

}
#ifdef COMPSS_WORKER
static void Block::test1( int size, float *out_array, int *in_array, double *inout_array, char* filename){
	for (int i=0; i<size;i++){
		if (i<MAX){
			out_array[i]=in_array[i];
		 }
	}
	ofstream ofs(filename);
	for (int i=0; i<30;i++){
		inout_array[i]++;
		ofs << inout_array << ":";
	}
	ofs.close();
}


static Block Block::create(int bSize, double initVal){
	cout << "Running create..." << endl << flush;
	Block b(bSize);
        cout << "Initializing block..." << endl << flush;
	for (int i=0; i<bSize; i++) {
                for (int j=0; j<bSize; j++) {
                        b.data[i][j] = initVal;
                }
        }
	b.print();
	cout << "Returning block" << endl << flush;

	return b;
}

void Block::test3(int size, float *in_array){
	cout<< "Reading array: " << endl;
	for (int i=0; i<size;i++){
                 cout << in_array[i] << ", ";
        }
	cout<< ": end array " << endl;
}

void Block::init(int bSize, double initVal) {
        M = bSize;
        data.resize(M);
        for (int i=0; i<M; i++) {
                data[i].resize(M);
        }
	for (int i=0; i<bSize; i++) {
                for (int j=0; j<bSize; j++) {
                        data[i][j] = initVal;
                }
        }
}

void Block::multiply(Block *block1, Block *block2) {
	cout << "Running create..." << endl << flush;
	for (int i=0; i<M; i++) {
		for (int j=0; j<M; j++) {
			for (int k=0; k<M; k++) {
				cout << data[i][j] << flush;
				cout << " " << block1->data[i][k] << flush; 
				cout << " * "<< block2->data[k][j] << endl <<flush;
				data[i][j] += block1->data[i][k] * block2->data[k][j];
			}
		}
	}
	cout << "End of create..." << endl << flush;
}

#endif

void Block::print() {
	for (int i=0; i<M; i++) {
		for (int j=0; j<M; j++) {
			cout << data[i][j] << " ";
		}
		cout << "\r\n";
	}
}

void Block::result() {
	cout << data[0][0] << "\r\n";
}
