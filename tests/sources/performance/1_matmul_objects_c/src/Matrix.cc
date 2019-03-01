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

//#include <GS_templates.h>

#include "Matrix.h"
#include <unistd.h>


Matrix::Matrix(int mSize) {
	N = mSize;
	data.resize(N);
	for (int i=0; i<N; i++) {
		data[i].resize(N);
	}
}


void Matrix::init(int mSize, int bSize, double val) {
	for (int i=0; i<mSize; i++) {
                for (int j=0; j<mSize; j++) {
                        data[i][j] = new Block(bSize); 
                 }
         }

}

void Matrix::print() {
	for (int i=0; i<N; i++) {
		for (int j=0; j<N; j++) {
			data[i][j]->print();
			cout << "\r\n";
		}
		cout << "\r\n";
	}
}

void Matrix::result(){
	data[0][0]->result();
}

#ifdef COMPSS_WORKER

void multiplyBlocks(Block *block1, Block *block2, Block *block3) {
	block1->multiply(*block2, *block3);
}

void initBlock(Block *block, int bSize, double initVal){
	block->init(bSize,initVal);
} 


#endif


