#ifndef GS_TEMPLATES_H
#define GS_TEMPLATES_H

// Uncomment the following define to get debug information.
#define DEBUG_BINDING

#ifdef DEBUG_BINDING
#define debug_printf(args...) printf(args)
#else
#define debug_printf(args...) {}
#endif

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
void GS_clean();
void compss_on(void);
void compss_off(void);
void compss_ifstream(char * filename, ifstream& ifs);
void compss_ofstream(char * filename, ofstream& ofs);
void compss_delete_file(char * filename);
void compss_barrier();
FILE* compss_fopen(char * filename, char * mode);
template <class T> void compss_wait_on(T &obj);
template <> inline void compss_wait_on<char *>(char * &obj);

#ifndef COMPSS_WORKER

template <class T>
void compss_wait_on(T &obj) {
  Entry entry = objectMap[&obj];
  char *runtime_filename;
  
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.type: %d\n", entry.type);
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.classname: %s\n", entry.classname);
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", entry.filename);
  
  GS_Get_File(entry.filename, in_dir, &runtime_filename);
  
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);
  
  ifstream ifs(runtime_filename);
  archive::text_iarchive ia(ifs);
  ia >> obj;
  ifs.close();
  
  // No longer needed, the current version of the object is in memory now
  remove(entry.filename);
  remove(runtime_filename);
  objectMap.erase(&obj);
}

template <>
void compss_wait_on<char *>(char * &obj) {
  string in_string;
  
  Entry entry = objectMap[&obj];
  char *runtime_filename;
  
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.type: %d\n", entry.type);
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.classname: %s\n", entry.classname);
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", entry.filename);
  
  GS_Get_File(entry.filename, in_dir, &runtime_filename);
  
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);
  
  if ((datatype)entry.type != file_dt) {
    debug_printf("[   BINDING]  -  @compss_wait_on  -  Object deserialization from %s\n", runtime_filename);
    
    ifstream ifs(runtime_filename);
    archive::text_iarchive ia(ifs);
    ia >> in_string;
    ifs.close();
    
    obj = strdup(in_string.c_str());
    
    // No longer needed, the current version of the object is in memory now
    remove(entry.filename);
    //remove(runtime_filename);
  } else {
    // Update file contents
    debug_printf("[   BINDING]  -  @compss_wait_on  -  File renaming: %s to %s\n", runtime_filename, entry.filename);
    remove(entry.filename);
    //rename(runtime_filename, entry.filename);
    symlink(runtime_filename, entry.filename);
  }
  // No longer needed, synchronization done
  objectMap.erase(&obj);
}

#else

template <class T> void compss_wait_on(T &obj) { }
template <> void compss_wait_on<char *>(char * &obj) { }

#endif /* COMPSS_WORKER */

#endif /* GS_TEMPLATES */
