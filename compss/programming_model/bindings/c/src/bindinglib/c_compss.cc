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

#include "c_compss.h"

using namespace std;
using namespace boost;

map<void *, Entry> objectMap;
CBindingCache * cache = NULL;
void compss_on(void) {
    cache = new CBindingCache();
    GS_On(cache);
}

void compss_off(void) {
    GS_Off(0);
    compss_clean();
    delete(cache);
}

void compss_off(int code) {
    GS_Off(code);
    compss_clean();
    delete(cache);
}

void compss_cancel_application_tasks() {
    //long l_app_id = (long)app_id;
    long int l_app_id = 0;
    GS_Cancel_Application_Tasks(l_app_id);
}

void compss_clean() {
    std::map<void *, Entry>::iterator it;
    for (std::map<void *,Entry>::iterator it=objectMap.begin(); it!=objectMap.end(); ++it) {
        remove (it->second.filename);
    }
}

void compss_ifstream(char * filename, ifstream& ifs) {
    char *runtime_filename;
    long int l_app_id = 0;

    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", filename);

    GS_Open_File(l_app_id, filename, in_dir, &runtime_filename);

    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);

    ifs.open(runtime_filename);
}

void compss_ofstream(char * filename, ofstream& ofs) {
    char *runtime_filename;
    long int l_app_id = 0;

    debug_printf("[C-BINDING]  -  @compss_ofstream  -  Entry.filename: %s\n", filename);

    GS_Open_File(l_app_id, filename, out_dir, &runtime_filename);

    debug_printf("[C-BINDING]  -  @compss_ofstream  -  Runtime filename: %s\n", runtime_filename);

    ofs.open(runtime_filename);
}

FILE* compss_fopen(char * filename, char * mode) {
    char *runtime_filename;
    FILE* file;
    enum direction dir;
    long int l_app_id = 0;

    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", filename);

    if (strcmp(mode, "r") == 0) {
        dir = in_dir;
    } else if (strcmp(mode, "w") == 0) {
        dir = out_dir;
    } else if (strcmp(mode, "a") == 0) {
        dir = inout_dir;
    } else if (strcmp(mode, "r+") == 0) {
        dir = inout_dir;
    } else if (strcmp(mode, "w+") == 0) {
        dir = out_dir;
    } else if (strcmp(mode, "a+") == 0) {
        dir = inout_dir;
    } else if (strcmp(mode, "c") == 0) {
        dir = concurrent_dir;
    } else if (strcmp(mode, "cv") == 0) {
        dir = commutative_dir;
    }

    GS_Open_File(l_app_id, filename, dir, &runtime_filename);

    debug_printf("[C-BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);

    file = fopen(runtime_filename, mode);

    return file;
}

void compss_delete_file(char *filename) {
    long int l_app_id = 0;
    GS_Delete_File(l_app_id, filename, 0, 1);
    return;
}

void compss_wait_on_file(char *filename) {
    long int l_app_id = 0;
    GS_Get_File(l_app_id, filename);
    return;
}

void compss_barrier() {
    //long l_app_id = (long)app_id;
    long int l_app_id = 0;
    GS_Barrier(l_app_id);
}

void compss_barrier_new(int no_more_tasks) {
    //long l_app_id = (long)app_id;
    long int l_app_id = 0;
    GS_BarrierNew(l_app_id, no_more_tasks);
}

void compss_barrier_group(char *groupname) {
    long int l_app_id = 0;
    char *exception_message = NULL;
    GS_BarrierGroup(l_app_id, groupname,&exception_message);
}

void compss_open_group(char *groupname, int implicitBarrier) {
    long int l_app_id = 0;
    GS_OpenTaskGroup(groupname, implicitBarrier, l_app_id);
}

void compss_close_group(char *groupname) {
    long int l_app_id = 0;
    GS_CloseTaskGroup(groupname, l_app_id);
}

void compss_cancel_group(char *groupname) {
    long int l_app_id = 0;
    char *exception_message = NULL;
    GS_CancelTaskGroup(groupname, l_app_id, &exception_message);
}



int compss_register(void *ref, datatype type, direction dir, char *classname, char * &filename, int object_type, int elements) {
    Entry entry;
    int result = 0;

    debug_printf("[C-BINDING]  -  @GS_register  -  Ref: %p\n", ref);

    if (dir == null_dir) {
        debug_printf("[C-BINDING]  -  @GS_register  -  Direction is null. Setting to out by default \n");
        dir = out_dir;
    }

    if (dir != in_dir && dir != concurrent_dir) {
        // OUT / INOUT. Create new version
        entry = objectMap[ref];
        if (entry.filename == NULL) {
            debug_printf("[C-BINDING]  -  @GS_register  -  ENTRY ADDED\n");
            entry.type = type;
            entry.classname = strdup(classname);
            entry.object_type = object_type;
            entry.elements = elements;
            if ((datatype)entry.type != file_dt) {
                debug_printf("[C-BINDING]  -  @GS_register  -  Assigning a filename to parameter \n");
                entry.filename =  strdup("compss-serialized-obj_XXXXXX");
                //TODO: Maybe not required (just generate UUID)
                int fd = mkstemp(entry.filename);
                if (fd== -1) {
                    printf("[C-BINDING]  -  @GS_register  -  ERROR creating temporal file\n");
                    return 1;
                }
            } else {
                debug_printf("[C-BINDING]  -  @GS_register  -  Registering File \n");
                entry.filename = strdup(filename);
            }

            objectMap[ref] = entry;

        } else {
            debug_printf("[C-BINDING]  -  @GS_register  -  ENTRY FOUND\n");
            result = 1;
        }

        debug_printf("[C-BINDING]  -  @GS_register  -  Entry.type: %d\n", entry.type);
        debug_printf("[C-BINDING]  -  @GS_register  -  Entry.classname: %s\n", entry.classname);
        debug_printf("[C-BINDING]  -  @GS_register  -  Entry.filename: %s\n", entry.filename);
        debug_printf("[C-BINDING]  -  @GS_register  -  Entry.object_type: %d\n", entry.object_type);
        debug_printf("[C-BINDING]  -  @GS_register  -  Entry.elements: %d\n", entry.elements);
        filename = strdup(entry.filename);
        debug_printf("[C-BINDING]  -  @GS_register  -  setting filename: %s\n", filename);

    } else {
        // IN or CONCURRENT
        if ((datatype)type == object_dt ||
                ((datatype)type >= array_char_dt && (datatype)type <= array_double_dt )) {

            entry = objectMap[ref];

            if (entry.filename == NULL) {
                debug_printf("[C-BINDING]  -  @GS_register  -  ENTRY ADDED\n");
                debug_printf("[C-BINDING]  -  @GS_register  -  Assigning a filename to parameter \n");
                entry.type = type;
                entry.classname = strdup(classname);
                entry.object_type = object_type;
                entry.elements = elements;
                entry.filename =  strdup("compss-serialized-obj_XXXXXX");
                int fd = mkstemp(entry.filename);
                if (fd== -1) {
                    printf("[C-BINDING]  -  @GS_register  -  ERROR creating temporal file\n");
                    return 1;
                }
                objectMap[ref] = entry;
            } else {
                debug_printf("[C-BINDING]  -  @GS_register  -  ENTRY FOUND\n");
                result = 1;
            }

            debug_printf("[C-BINDING]  -  @GS_register  -  Entry.type: %d\n", entry.type);
            debug_printf("[C-BINDING]  -  @GS_register  -  Entry.classname: %s\n", entry.classname);
            debug_printf("[C-BINDING]  -  @GS_register  -  Entry.filename: %s\n", entry.filename);
            debug_printf("[C-BINDING]  -  @GS_register  -  Entry.object_type: %d\n", entry.object_type);
            debug_printf("[C-BINDING]  -  @GS_register  -  Entry.elements: %d\n", entry.elements);
            filename = strdup(entry.filename);
            debug_printf("[C-BINDING]  -  @GS_register  -  setting filename: %s\n", filename);
        } else {
            debug_printf("[C-BINDING]  -  @GS_register  -  File or Basic type parameter \n");
        }
    }

    debug_printf("[C-BINDING]  -  @GS_register  -  Filename: %s\n", filename);
    debug_printf("[C-BINDING]  -  @GS_register  - Result is %d\n", result);
    return result;
}
