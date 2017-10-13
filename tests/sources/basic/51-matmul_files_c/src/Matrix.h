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

#ifndef MATRIX_H
#define MATRIX_H

#include <stdio.h>
#include <vector>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/vector.hpp>

#include "Block.h"

using namespace std;
using namespace boost;
using namespace serialization;

class Matrix {

public:
	std::vector< std::vector< Block* > > data;
	Matrix(){};

	Matrix(int mSize, int bSize, char mat_name);

	void init(int mSize, int bSize, double val, char mat_name);

	void multiply(Matrix matrix1, Matrix matrix2);

	void print();

	void result();

	static Block *get_block(char *file, int M);

	static void write_block(Block *b, char *file, int M);

private:
	int N;
	int M;
	int matName;

	friend class::serialization::access;

	template<class Archive>
	void serialize(Archive & ar, const unsigned int version) {
		ar & N;
		ar & data;
	}
};

#endif
