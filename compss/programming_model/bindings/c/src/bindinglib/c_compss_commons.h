/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
//#ifdef TEXT_SERIALIZATION
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
//#else
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
//#endif
#include <boost/iostreams/stream_buffer.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/device/back_inserter.hpp>

#include <BindingDataManager.h>
#include <common.h>
#include "CBindingCache.h"


using namespace std;
using namespace boost;


/*template <class T> void compss_object_deserialize(T &obj, char* filename, bool is_string);
template <class T> void compss_object_serialize(T &obj, char* filename, bool is_string);
template <class T> void compss_array_deserialize(T &obj, char* filename, int elements);
template <class T> void compss_array_serialize(T &obj, char* filename, int elements);
*/

template <class T>
int compss_object_deserialize(T* obj, const char* filename) {
    debug_printf("[C-BINDING]  -  @compss_object_deserialize  -  Ref: %p to file %s\n", obj, filename);
#ifdef TEXT_SERIALIZATION
    //Text serialization
    ifstream ifs(filename);
    try {
        archive::text_iarchive ia(ifs);
#else
    //Binary serialization
    ifstream ifs(filename, ios::binary );
    try {
        archive::binary_iarchive ia(ifs);
#endif
        ia >> *obj;
        ifs.close();
        debug_printf("[C-BINDING]  -  @compss_object_deserialize  - Object deserialized\n");
        return 0;
    } catch(archive::archive_exception e) {
        debug_printf("[C-BINDING]  -  Error deserializing obj %p\n", obj);
        const char *message = e.what();
        debug_printf("Exception: %s ", message);
        ifs.close();
        return 1;
    }

}

template <class T>
int compss_object_serialize(T* obj, const char* filename) {
    debug_printf("[C-BINDING]  -  @compss_object_serialize  -  Ref: %p to file %s\n", obj, filename);

#ifdef TEXT_SERIALIZATION
    //Text serialization
    ofstream ofs(filename, std::ofstream::trunc);
    try {
        archive::text_oarchive oa(ofs);
#else
    //Binary serialization
    ofstream ofs(filename, std::ofstream::trunc | std::ios::binary);
    try {
        archive::binary_oarchive oa(ofs);
#endif
        oa << *obj;
        //ofs.flush();
        ofs.close();
        return 0;
    } catch(archive::archive_exception e) {
        printf("[C-BINDING]  -  Error serializing obj %p\n", obj);
        const char* message = e.what();
        printf("[C-BINDING]  -  Exception: %s\n", message);
        ofs.close();
        return 1;
    }
}

template <class T>
int compss_object_deserialize(T* obj, JavaNioConnStreamBuffer &jsb) {
    debug_printf("[C-BINDING]  -  @compss_object_deserialize  -  Ref: %p to JavaNioBuffer\n", obj);
    istream ijs(&jsb);
    try {
#ifdef TEXT_SERIALIZATION
        debug_printf("[C-BINDING]  -  Text deserialization\n");
        //Text serialization
        archive::text_iarchive ia(ijs);
#else
        debug_printf("[C-BINDING]  -  Binary deserialization\n");
        //Binary serialization
        archive::binary_iarchive ia(ijs);
#endif
        ia >> *obj;
        return 0;
    } catch(archive::archive_exception e) {
        printf("[C-BINDING]  -  Error deserializing object %p to JavaNioBuffer\n", obj);
        const char* message = e.what();
        printf("[C-BINDING]  -  Exception: %s\n", message);
        return 1;
    }
}

template <class T>
int compss_object_serialize(T* obj, JavaNioConnStreamBuffer &jsb) {
    debug_printf("[C-BINDING]  -  @compss_object_serialize  -  Ref: %p to JavaNioBuffer\n", obj);
    ostream ojs(&jsb);
    try {
#ifdef TEXT_SERIALIZATION
        debug_printf("[C-BINDING]  -  Text serialization\n");
        //Text serialization
        archive::text_oarchive oa(ojs);
#else
        debug_printf("[C-BINDING]  - Binary serialization\n");
        //Binary serialization
        archive::binary_oarchive oa(ojs);
#endif
        oa << *obj;
        //ojs.flush();
        return 0;
    } catch(archive::archive_exception e) {
        printf("[C-BINDING] Error serializing object %p to JavaNioBuffer\n", obj);
        const char* message = e.what();
        printf("[C-BINDING]  -  Exception: %s\n", message);
        return 1;
    }
}

template <class T>
int compss_object_copy(T* from, T* to) {
    typedef std::vector<char> buffer_type;
    buffer_type buffer;
    boost::iostreams::stream<boost::iostreams::back_insert_device<buffer_type> > os(buffer);
    try {
/*#ifdef TEXT_SERIALIZATION
        //Text serialization
        archive::text_oarchive oa(os);
#else*/
        //Binary serialization
        archive::binary_oarchive oa(os);
//#endif
        oa << *from;
        os.flush();

        iostreams::basic_array_source<char> source(&buffer[0],buffer.size());
        iostreams::stream_buffer<iostreams::basic_array_source <char> > is(source);

/*#ifdef TEXT_SERIALIZATION
        //Text serialization
        archive::text_iarchive ia(is);
#else*/
        //Binary serialization
        archive::binary_iarchive ia(is);
//#endif
        ia >> *to;
        return 0;
    } catch(archive::archive_exception e) {
        printf("[C-BINDING]  -  Error copying %p to %p\n", &from, &to);
        const char* message = e.what();
        printf("[C-BINDING]  -  Exception: %s\n", message);
        return 1;
    }
}


template <class T> int compss_array_deserialize(T* &to, const char* filename, int elements) {
    debug_printf("[C-BINDING]  -  @compss_array_deserialize  -  Ref: %p to file %s\n", to, filename);
    to = new T[elements];
    to = (T*)malloc(sizeof(T)*elements);
    ifstream ifs(filename, ios::binary);
    ifs.read((char*)to, sizeof(T)*elements);
    ifs.close();
    debug_printf("[C-BINDING]  -  @compss_array_deserialize  -  Array deserialized in ref: %p.\n", to);
    return 0;
}

template <class T> int compss_array_serialize(T* obj, const char* filename, int elements) {
    debug_printf("[C-BINDING]  -  @compss_array_serialize  -  Ref: %p to file %s\n", obj, filename);
    ofstream ofs(filename, std::ofstream::trunc | std::ios::binary);
    ofs.write((char*)obj, sizeof(T)*elements);
    ofs.close();
    debug_printf("[C-BINDING]  -  @compss_array_serialize  -  Array serialized.\n");
    return 0;
}

template <class T> int compss_array_deserialize(T* &to, JavaNioConnStreamBuffer &jsb, int elements) {
    debug_printf("[C-BINDING]  -  @compss_array_deserialize  -  Ref: %p to JavaNioBuffer\n", to);
    to = new T[elements];
    //to = (T*)malloc(sizeof(T)*elements);
    istream ijs(&jsb);
    ijs.read((char*)to, sizeof(T)*elements);
    return 0;
}

template <class T> int compss_array_serialize(T* obj, JavaNioConnStreamBuffer &jsb, int elements) {
    debug_printf("[C-BINDING]  -  @compss_array_serialize  -  Ref: %p to JavaNioBuffer\n", obj );
    ostream ojs(&jsb);
    ojs.write((char*)obj, sizeof(T)*elements);
    ojs.flush();
    return 0;
}

template <class T> int compss_array_copy(T* from, T* &to, int elements) {
    to = new T[elements];
	//to = (T*)malloc(sizeof(T)*elements);
    debug_printf("[C-BINDING]  -  @compss_array_copy  -  Ref: %p to %p\n", &from, &to);
    memcpy((void*)to, (void*)from, sizeof(T)*elements);
    return 0;
}

#endif /* C_COMPSS_COMMONS_H */
