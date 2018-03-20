/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
 *
 */
#ifndef C_COMPSS_COMMONS_H
#define C_COMPSS_COMMONS_H

#include <stdio.h>
#include <stdlib.h>

#include <fstream>
#include <iostream>
#include <vector>
#include <map>
#include <string>
#include <string.h>
#include <JavaNioConnStreamBuffer.h>
#ifdef TEXT_SERIALIZATION
	#include <boost/archive/text_iarchive.hpp>
	#include <boost/archive/text_oarchive.hpp>
#else
	#include <boost/archive/binary_iarchive.hpp>
	#include <boost/archive/binary_oarchive.hpp>
#endif
#include <boost/iostreams/stream_buffer.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

#include "CBindingCache.h"

#ifdef DEBUG_BINDING
#define debug_printf(args...) printf(args); fflush(stdout);
#else
#define debug_printf(args...) {}
#endif

using namespace std;
using namespace boost;


/*template <class T> void compss_object_deserialize(T &obj, char* filename, bool is_string);
template <class T> void compss_object_serialize(T &obj, char* filename, bool is_string);
template <class T> void compss_array_deserialize(T &obj, char* filename, int elements);
template <class T> void compss_array_serialize(T &obj, char* filename, int elements);
*/

template <class T>
void compss_object_deserialize(T &obj, const char* filename) {
	debug_printf("[C-BINDING]  -  @compss_object_deserialize  -  Ref: %p to file %s\n", &obj, filename);
#ifdef TEXT_SERIALIZATION
	//Text serialization
	ifstream ifs(filename);
	archive::text_iarchive ia(ifs);
#else
	//Binary serialization
	ifstream ifs(filename, ios::binary );
	archive::binary_iarchive ia(ifs);
#endif
	ia >> obj;
	ifs.close();

}

template <class T>
void compss_object_serialize(T &obj, const char* filename) {
	debug_printf("[C-BINDING]  -  @compss_object_serialize  -  Ref: %p to file %s\n", &obj, filename);
#ifdef TEXT_SERIALIZATION
	//Text serialization
	ofstream ofs(filename, std::ofstream::trunc);
	archive::text_oarchive oa(ofs);
#else
	//Binary serialization
	ofstream ofs(filename, std::ofstream::trunc | std::ios::binary);
	archive::binary_oarchive oa(ofs);
#endif
	oa << obj;
	//ofs.flush();
	ofs.close();
}

template <class T>
void compss_object_deserialize(T &obj, JavaNioConnStreamBuffer &jsb) {
	debug_printf("[C-BINDING]  -  @compss_object_deserialize  -  Ref: %p to JavaNioBuffer\n", &obj);
	istream ijs(&jsb);
#ifdef TEXT_SERIALIZATION
	//Text serialization
	archive::text_iarchive ia(ijs);
#else
	//Binary serialization
	archive::binary_iarchive ia(ijs);
#endif
	ia >> obj;
}

template <class T>
void compss_object_serialize(T &obj, JavaNioConnStreamBuffer &jsb) {
	debug_printf("[C-BINDING]  -  @compss_object_serialize  -  Ref: %p to JavaNioBuffer\n", &obj);
	ostream ojs(&jsb);
#ifdef TEXT_SERIALIZATION
	//Text serialization
	archive::text_oarchive oa(ojs);
#else
	//Binary serialization
	archive::binary_oarchive oa(ojs);
#endif
	oa << obj;
	//ojs.flush();
}

template <class T>
void compss_object_copy(T &from, T&to){
	typedef std::vector<char> buffer_type;
	buffer_type buffer;
	boost::iostreams::stream<boost::iostreams::back_insert_device<buffer_type> > os(buffer);

#ifdef TEXT_SERIALIZATION
	//Text serialization
	archive::text_oarchive oa(os);
#else
	//Binary serialization
	archive::binary_oarchive oa(os);
#endif
	oa << from;
	os.flush();

	iostreams::basic_array_source<char> source(&buffer[0],buffer.size());
	iostreams::stream<iostreams::basic_array_source <char> > is(source);

#ifdef TEXT_SERIALIZATION
	//Text serialization
	archive::text_iarchive ia(is);
#else
	//Binary serialization
	archive::binary_iarchive ia(is);
#endif
	ia >> to;
}

/*void compss_string_serialize(string &obj, char* filename) {
	debug_printf("[C-BINDING]  -  @compss_string_serialize  -  Ref: %p to file %s\n", &obj, filename);
#ifdef TEXT_SERIALIZATION
	//Text serialization
	ofstream ofs(filename, std::ofstream::trunc);
	archive::text_oarchive oa(ofs);
#else
	//Binary serialization
	ofstream ofs(filename, std::ofstream::trunc | std::ios::binary);
	archive::binary_oarchive oa(ofs);
#endif
	string out_string (obj);
	oa << out_string;
	//ofs.flush();
	ofs.close();
}

void compss_string_deserialize(string &obj, char* filename) {
	debug_printf("[C-BINDING]  -  @compss_string_deserialize  -  Ref: %p to file %s\n", &obj, filename);
#ifdef TEXT_SERIALIZATION
	//Text serialization
	ifstream ifs(filename);
	archive::text_iarchive ia(ifs);
#else
	//Binary serialization
	ifstream ifs(filename, ios::binary );
	archive::binary_iarchive ia(ifs);
#endif

	string in_string;
	ia >> in_string;
	ifs.close();
	obj = strdup(in_string.c_str());
}*/

template <class T> void compss_array_deserialize(T &obj, const char* filename, int elements){
	debug_printf("[C-BINDING]  -  @compss_array_deserialize  -  Ref: %p to file %s\n", &obj, filename);
	ifstream ifs(filename, ios::binary);
	ifs.read((char*)obj, sizeof(T)*elements);
	ifs.close();
}

template <class T> void compss_array_serialize(T &obj, const char* filename, int elements){
	debug_printf("[C-BINDING]  -  @compss_array_serialize  -  Ref: %p to file %s\n", &obj, filename);
	ofstream ofs(filename, std::ofstream::trunc | std::ios::binary);
	ofs.write((char*)obj, sizeof(T)*elements);
	ofs.close();
}

template <class T> void compss_array_deserialize(T &obj, JavaNioConnStreamBuffer &jsb, int elements){
	debug_printf("[C-BINDING]  -  @compss_array_deserialize  -  Ref: %p to JavaNioBuffer\n", &obj);
	istream ijs(&jsb);
	ijs.read((char*)obj, sizeof(T)*elements);
}

template <class T> void compss_array_serialize(T &obj, JavaNioConnStreamBuffer &jsb, int elements){
	debug_printf("[C-BINDING]  -  @compss_array_serialize  -  Ref: %p to JavaNioBuffer\n", &obj );
	ostream ojs(&jsb);
	ojs.write((char*)obj, sizeof(T)*elements);
	ojs.flush();
}

template <class T> void compss_array_copy(T &from, T &to, int elements){
	debug_printf("[C-BINDING]  -  @compss_arrat_serialize  -  Ref: %p to file %s\n", &obj, filename);
	to = (T)malloc(sizeof(T)*elements);
	memcpy((void*)to, (void*)from, sizeof(T)*elements);
}

#endif /* C_COMPSS_COMMONS_H */
