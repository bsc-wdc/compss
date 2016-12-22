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
