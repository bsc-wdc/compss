#include "GS_templates.h"

using namespace std;
using namespace boost;

map<void *, Entry> objectMap;

void compss_on(void) {
  GS_On();
}

void compss_off(void) {
  GS_Off();
  GS_clean();
}

void GS_clean() {
  std::map<void *, Entry>::iterator it;
  
  for (std::map<void *,Entry>::iterator it=objectMap.begin(); it!=objectMap.end(); ++it) {
    remove (it->second.filename);
  }
}


void compss_ifstream(char * filename, ifstream& ifs) {
  char *runtime_filename;

  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", filename);

  GS_Get_File(filename, in_dir, &runtime_filename);

  debug_printf("[   BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);

  ifs.open(runtime_filename);
}


void compss_ofstream(char * filename, ofstream& ofs) {
  char *runtime_filename;

  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", filename);

  GS_Get_File(filename, out_dir, &runtime_filename);

  debug_printf("[   BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);

  ofs.open(runtime_filename);
}


FILE* compss_fopen(char * filename, char * mode) {

  char *runtime_filename;
  FILE* file;
  enum direction dir;
 

  debug_printf("[   BINDING]  -  @compss_wait_on  -  Entry.filename: %s\n", filename);
  
  if (strcmp(mode, "r") == 0){
	dir = in_dir;
  }
  else if (strcmp(mode, "w") == 0){
        dir = out_dir;
  }
  else if (strcmp(mode, "a") == 0){
        dir = inout_dir;
  }
  else if (strcmp(mode, "r+") == 0){
        dir = inout_dir;
  }
  else if (strcmp(mode, "w+") == 0){
        dir = out_dir;
  }
  else if (strcmp(mode, "a+") == 0){
        dir = inout_dir;
  }  


  GS_Get_File(filename, dir, &runtime_filename);
  
  debug_printf("[   BINDING]  -  @compss_wait_on  -  Runtime filename: %s\n", runtime_filename);
  
  file = fopen(runtime_filename, mode);
   

  return file;

}


int delete_file(char * filename)
{
    int *result;
    
    GS_Delete_File(filename, &result);

    return *result;
}


void waitForAllTasks()
{

    //long l_app_id = (long)app_id;
    long int l_app_id = 0;
    GS_Barrier(l_app_id);

}


int GS_register(void *ref, datatype type, direction dir, char *classname, char * &filename) {
  Entry entry;
  int result = 0;
  
  debug_printf("[   BINDING]  -  @GS_register  -  Ref: %p\n", (char *)ref);
  
  if (dir == null_dir) {
    debug_printf("[   BINDING]  -  @GS_register  -  Direction is null \n"); 
    dir = out_dir;
  }
  
  if (dir != in_dir) {
    // OUT / INOUT. Create new version
    entry = objectMap[ref];
    if (entry.filename == NULL) {
      debug_printf("[   BINDING]  -  @GS_register  -  ENTRY ADDED\n");
      entry.type = type;
      entry.classname = strdup(classname);
      
      if ((datatype)entry.type != file_dt) {
	entry.filename =  strdup("compss-serialized-obj_XXXXXX");
	int fd = mkstemp(entry.filename);
	if (fd== -1){
	  printf("[   BINDING]  -  @GS_register  -  ERROR creating temporal file\n");
	  return 1;
	}		
      } else {
	entry.filename = strdup(filename);
      }
      
      objectMap[ref] = entry;
   
    } else {
      debug_printf("[   BINDING]  -  @GS_register  -  ENTRY FOUND\n");
    }
    
    debug_printf("[   BINDING]  -  @GS_register  -  Entry.type: %d\n", entry.type);
    debug_printf("[   BINDING]  -  @GS_register  -  Entry.classname: %s\n", entry.classname);
    debug_printf("[   BINDING]  -  @GS_register  -  Entry.filename: %s\n", entry.filename);
    
    filename = strdup(entry.filename);
    debug_printf("[   BINDING]  -  @GS_register  -  setting filename: %s\n", filename);
    
  } else {
    // IN
    if ((datatype)type == object_dt) {
      entry = objectMap[ref];
      
      if (entry.filename == NULL) {
	debug_printf("[   BINDING]  -  @GS_register  -  ENTRY ADDED\n");
	entry.type = type;
	entry.classname = strdup(classname);
	entry.filename =  strdup("compss-serialized-obj_XXXXXX");
	int fd = mkstemp(entry.filename);
	if (fd== -1){
	  printf("[   BINDING]  -  @GS_register  -  ERROR creating temporal file\n");
	  return 1;
	}
	
	
	objectMap[ref] = entry;
      } else {
	debug_printf("[   BINDING]  -  @GS_register  -  ENTRY FOUND\n");
      }
      
      debug_printf("[   BINDING]  -  @GS_register  -  Entry.type: %d\n", entry.type);
      debug_printf("[   BINDING]  -  @GS_register  -  Entry.classname: %s\n", entry.classname);
      debug_printf("[   BINDING]  -  @GS_register  -  Entry.filename: %s\n", entry.filename);
      
      filename = strdup(entry.filename);
      debug_printf("[   BINDING]  -  @GS_register  -  setting filename: %s\n", filename);
    }
  }
  
  debug_printf("[   BINDING]  -  @GS_register  -  Filename: %s\n", filename);
  debug_printf("[   BINDING]  -  @GS_register  - Result is %d\n", result);
  return result;
}
