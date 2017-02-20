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

#include "Matmul.h"
#include "Matrix.h"
#include "Block.h"

/*
void initMatrix(Matrix *matrix, int mSize, int nSize, double val) {
	*matrix = Matrix::init(mSize, nSize, val);
}


void multiplyBlocks(Block *block1, Block *block2, Block *block3) {
	block1->multiply(*block2, *block3);
}
*/
