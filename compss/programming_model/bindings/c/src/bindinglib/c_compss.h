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

#ifndef GS_TEMPLATES_H
#define GS_TEMPLATES_H

#include <stdio.h>
#include <stdlib.h>

#include <GS_compss.h>
#include <param_metadata.h>
#include "c_compss_commons.h"

#ifndef COMPSS_WORKER
#include <BindingDataManager.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <map>
#include <string>
#include <string.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>

//#include <executor.h>

using namespace std;
using namespace boost;

struct Entry {
    datatype type;
    char *classname;
    char *filename;
    int object_type;
    int elements;
};
extern map<void *, Entry> objectMap;

#endif /* COMPSS_WORKER */


int compss_register(void *ref, datatype type, direction dir, char *classname, char * &filename, int object_type, int elements);
void compss_clean();
void compss_on(void);
void compss_off(void);
void compss_off(int code);
void compss_cancel_applicatin_tasks();

void compss_ifstream(char * filename, ifstream& ifs);
void compss_ofstream(char * filename, ofstream& ofs);
void compss_delete_file(char * filename);
FILE* compss_fopen(char * filename, char * mode);
void compss_wait_on_file(char *filename);
void compss_barrier();
void compss_barrier_new(int no_more_tasks);
void compss_barrier_group(char *groupname);
void compss_open_group(char *groupname, int implicitBarrier);
void compss_close_group(char *groupname);
void compss_cancel_group(char *groupname);

template <class T> void compss_wait_on(T* &obj);
template <class T> T compss_wait_on(T &obj);
template <class T> int compss_delete_object(T* &obj);


#ifndef COMPSS_WORKER

template <class T> int compss_delete_object(T* &obj) {
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Ref: %p\n", obj);
    Entry entry = objectMap[obj];

    if (entry.classname == NULL) {
        printf("[C-BINDING]  -  WARNING  -  @delete_object  -  deleting object which has not been accessed by a task.\n"
               "\tEither delete object is not needed or object pointer is not correctly passed.\n");
        return 0;
    } else {
        int res = delete_object_from_runtime(entry.filename, entry.object_type, entry.elements);
        remove(entry.filename);
        objectMap.erase(obj);
        return res;
    }
}

template <class T> T compss_wait_on(T& obj) {
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Ref: %p\n", &obj);
    Entry entry = objectMap[&obj];
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.type: %d\n", entry.type);
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.classname: %s\n", entry.classname);
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", entry.filename);
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.object_type: %d\n", entry.object_type);
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.object_type: %d\n", entry.elements);

    if (entry.classname == NULL) {
        printf("[C-BINDING]  -  WARNING  -  @compss_wait_on  -  Waiting on an object which has not been accessed by a task.\n"
               "\tEither compss_wait_on is not needed or object pointer is not correctly passed.\n");
        return obj;
    } else {
        debug_printf("[C-BINDING]  -  @compss_wait_on  -  sync object %s to runtime\n", entry.filename);
        long int l_app_id = 0;
        T* new_obj = (T*)sync_object_from_runtime(l_app_id, entry.filename, entry.object_type, entry.elements);
        remove(entry.filename);
        objectMap.erase(&obj);
        debug_printf("[C-BINDING]  -  @compss_wait_on  -  object synchronized\n");
        return (*new_obj);
    }
}

template <class T>
void compss_wait_on(T* &obj) {
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Ref: %p\n", obj);
    Entry entry = objectMap[obj];
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.type: %d\n", entry.type);
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.classname: %s\n", entry.classname);
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", entry.filename);
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.object_type: %d\n", entry.object_type);
    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.elements: %d\n", entry.elements);

    if (entry.classname == NULL) {
        printf("[C-BINDING]  -  WARNING  -  @compss_wait_on  -  Waiting on an object which has not been accessed by a task.\n"
               "\tEither compss_wait_on is not needed or object pointer is not correctly passed.\n");
    } else {
        debug_printf("[C-BINDING]  -  @compss_wait_on  -  sync object %s to runtime\n", entry.filename);
        void* old_obj = obj;
        long int l_app_id = 0;
        obj = (T*)sync_object_from_runtime(l_app_id, entry.filename, entry.object_type, entry.elements);
        remove(entry.filename);
        objectMap.erase(old_obj);
        debug_printf("[C-BINDING]  -  @compss_wait_on  -  object synchronized\n");
    }
}

#else
template <class T> int compss_delete_object(T* &obj) { }
template <class T> void compss_wait_on(T* &obj) { }
template <class T> T compss_wait_on(T &obj) { }
//template <> void compss_wait_on<char *>(char * &obj) { }

#endif /* COMPSS_WORKER */

#endif /* GS_TEMPLATES */
