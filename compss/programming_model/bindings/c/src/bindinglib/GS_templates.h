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

#ifndef GS_TEMPLATES_H
#define GS_TEMPLATES_H

#include <stdio.h>
#include <stdlib.h>

#include <GS_compss.h>
#include <param_metadata.h>

#ifndef COMPSS_WORKER

#include <fstream>
#include <iostream>
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
};
extern map<void *, Entry> objectMap;

#endif /* COMPSS_WORKER */

int GS_register(void *ref, datatype type, direction dir, char *classname, char * &filename);

void compss_on(void);
void compss_off(void);
void GS_clean();

void compss_ifstream(char * filename, ifstream& ifs);
void compss_ofstream(char * filename, ofstream& ofs);
void compss_delete_file(char * filename);
FILE* compss_fopen(char * filename, char * mode);

void compss_barrier();
void compss_barrier_new(int no_more_tasks);

template <class T> void compss_wait_on(T &obj);
template <class T> void persistent_compss_wait_on(T &obj);
template <> inline void compss_wait_on<char *>(char * &obj);


#ifndef COMPSS_WORKER

template <class T>
void compss_wait_on(T &obj) {
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Ref: %p\n", &obj);
  Entry entry = objectMap[&obj];
  char *runtime_filename;
  
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.type: %d\n", entry.type);
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.classname: %s\n", entry.classname);
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", entry.filename);
  
  if (entry.classname == NULL) debug_printf("[C-BINDING]  -  ERROR  -  @compss_wait_on  -  Waiting on a null entry\n");

  GS_Get_File(entry.filename, in_dir, &runtime_filename);
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  template class\n");  
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);


  ifstream ifs(runtime_filename, ios::binary );  
  archive::binary_iarchive ia(ifs);

  ia >> obj;
  ifs.close();

  debug_printf("[C-BINDING]  -  @compss_wait_on  - File serialization finished\n");
  
  // No longer needed, the current version of the object is in memory now
  GS_Close_File(entry.filename, in_dir);
  compss_delete_file(entry.filename);
  remove(entry.filename);
  remove(runtime_filename);
  objectMap.erase(&obj);
}
/*
template <class T>
void persistent_compss_wait_on(T &obj) {
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Ref: %p\n", &obj);
  Entry entry = objectMap[&obj];
  char *runtime_filename;

  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.type: %d\n", entry.type);
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.classname: %s\n", entry.classname);
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", entry.filename);

  GS_Get_File(entry.filename, in_dir, &runtime_filename);
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  template class\n");
  debug_printf("[C-BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);

  cout << "before checking if file exists in compss wait" << endl;

//  while( !filesystem::exists(runtime_filename));

//  cout << "file exists? " << filesystem::exists(runtime_filename) << endl;

//  ifstream ifs(runtime_filename);

  ifstream ifs(runtime_filename);

  bool exists = false;

  while (false){
    exists = ifs.good();
    cout << "file exists? " << exists << endl;
  }

  cout << "about to serialize file in compss wait" << endl;

  archive::text_iarchive ia(ifs);

  cout << "archive created" << endl;

  ia >> obj;
  ifs.close();

  cout << "file serialised in compss wait" << endl;

  // No longer needed, the current version of the object is in memory now
  GS_Close_File(entry.filename, in_dir);
  compss_delete_file(entry.filename);
  remove(entry.filename);
  remove(runtime_filename);
  objectMap.erase(&obj);
}
*/

#else

template <class T> void compss_wait_on(T &obj) { }
template <class T> void persistent_compss_wait_on(T &obj) { }
template <> void compss_wait_on<char *>(char * &obj) { }

#endif /* COMPSS_WORKER */

#endif /* GS_TEMPLATES */
