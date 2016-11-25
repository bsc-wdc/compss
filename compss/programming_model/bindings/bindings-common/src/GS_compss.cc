#include <stdlib.h>
#include <jni.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>

#include "GS_compss.h"
#include "param_metadata.h"

// Uncomment the following define to get debug information.
// #define DEBUG_BINDING

#ifdef DEBUG_BINDING
#define debug_printf(args...) printf(args)
#else
#define debug_printf(args...) {}
#endif

using namespace std;

JNIEnv *env;
jobject jobjIT;
jclass clsITimpl;
JavaVM * jvm;

jobject appId;

jmethodID midAppDir; 		/* ID of the getApplicationDirectory method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */
jmethodID midExecute; 		/* ID of the executeTask method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */
jmethodID midRegisterCE; 	/* ID of the RegisterCE method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */
jmethodID midEmitEvent; 	/* ID of the EmitEvent method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */

jmethodID midOpenFile; 		/* ID of the openFile method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */
jmethodID midDeleteFile; 	/* ID of the deleteFile method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */

jmethodID midWaitForAllTasks; 	/* ID of the waitForAllTasks method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */

jobject jobjParDirIN; 		/* Instance of the integratedtoolkit.api.COMPSsRuntime$DataDirection class */
jobject jobjParDirINOUT; 	/* Instance of the integratedtoolkit.api.COMPSsRuntime$DataDirection class */
jobject jobjParDirOUT; 		/* Instance of the integratedtoolkit.api.COMPSsRuntime$DataDirection class */

jclass clsObject; 		/*  java.lang.Object class */
jmethodID midObjCon; 		/* ID of the java.lang.Object class constructor method */

jclass clsCharacter; 		/*  java.lang.Character class */
jmethodID midCharCon; 		/* ID of the java.lang.Character class constructor method */

jclass clsBoolean; 		/*  java.lang.Boolean class */
jmethodID midBoolCon; 		/* ID of the java.lang.clsBoolean class constructor method */

jclass clsShort; 		/*  java.lang.Short class */
jmethodID midShortCon; 		/* ID of the java.lang.Short class constructor method */

jclass clsInteger; 		/*  java.lang.Integer class */
jmethodID midIntCon; 		/* ID of the java.lang.Integer class constructor method */

jclass clsLong; 		/*  java.lang.Long class */
jmethodID midLongCon; 		/* ID of the java.lang.Long class constructor method */

jclass clsFloat; 		/*  java.lang.Float class */
jmethodID midFloatCon; 		/* ID of the java.lang.Float class constructor method */

jclass clsDouble; 		/*  java.lang.Double class */
jmethodID midDoubleCon; 	/* ID of the java.lang.Double class constructor method */


// Private functions

JNIEnv* create_vm(JavaVM ** jvm) {
  JNIEnv *env;
  JavaVMInitArgs vm_args;
  vector<JavaVMOption> options;
  
  string line;                                        // buffer for line read
  const char *file = strdup(getenv("JVM_OPTIONS_FILE")); // path to the file with jvm options
  ifstream fin;                                           // input file stream
  
  fin.open(file);
  if (fin.good()) {
    while (!fin.eof()) {
      // read in one line at a time
      getline(fin, line);
      // read data from file
      string fileOption = strdup(line.data());
      JavaVMOption *option = new JavaVMOption();
      
      int begin;
      int end;
      while ((begin = fileOption.find("$")) != fileOption.npos) {
	// It refers to an environment variable
	end = fileOption.find(":", begin);
	if (end == fileOption.npos)
	  end = fileOption.find("/", begin);
	string prefix = fileOption.substr(0, begin);
	string env_varName = fileOption.substr(begin + 1,
					       end - begin - 1);
	
	char *buffer = getenv(env_varName.data());
	
	if (buffer == NULL)
	  debug_printf("[   BINDING]  -  @create_vm  -  Cannot find environment variable: %s\n", env_varName.data());
	
	string env_varValue(buffer);
	string suffix = "";
	if (end != fileOption.npos)
	  suffix = fileOption.substr(end);
	fileOption.clear();
	fileOption.append(prefix);
	fileOption.append(env_varValue);
	fileOption.append(suffix);
      }
      
      if (fileOption.find("-") == 0) {
	// It is a JVM option
	option->optionString = strdup(fileOption.data());
	options.push_back(*option);
	debug_printf("[   BINDING]  -  @create_vm  -  option %s\n", option->optionString);
      } else {
	// It is an environment variable
	int ret = putenv(strdup(fileOption.data()));
	if (ret < 0) {
	  debug_printf("[   BINDING]  -  @create_vm  -  Cannot put environment variable: %s", fileOption.data());
	} else {
	  int begin = fileOption.find("=");
	  string env_varName = fileOption.substr(0, begin);
	  char *buffer = getenv(env_varName.data());
	  if (buffer == NULL)
	    debug_printf("[   BINDING]  -  @create_vm  -  Cannot find environment variable: %s\n", env_varName.data());
	}
      }
      fflush(stdout);
      fileOption.clear();
    }
  } else {
    debug_printf("[   BINDING]  -  @create_vm  -  JVM option file not good!\n");
  }
  // close file
  fin.close();
  
  vm_args.version = JNI_VERSION_1_8; //JDK version. This indicates version 1.8
  vm_args.nOptions = options.size();
  vm_args.options = new JavaVMOption[vm_args.nOptions];
  copy(options.begin(), options.end(), vm_args.options);
  vm_args.ignoreUnrecognized = false;
  
  int ret = JNI_CreateJavaVM(jvm, (void**) &env, &vm_args);
  if (ret < 0){
    debug_printf("[   BINDING]  -  @create_vm  -  Unable to Launch JVM - %i\n", ret);
  }
  return env;
}


void init_jni_types() {
  jclass clsParDir; 		/* integratedtoolkit.api.COMPSsRuntime$DataDirection class */
  jmethodID midParDirCon; 	/* ID of the integratedtoolkit.api.COMPSsRuntime$DataDirection class constructor method */
  
  // getApplicationDirectory method
  midAppDir = env->GetMethodID(clsITimpl, "getApplicationDirectory", "()Ljava/lang/String;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  // executeTask method
  midExecute = env->GetMethodID(clsITimpl, "executeTask", "(Ljava/lang/Long;Ljava/lang/String;Ljava/lang/String;ZZI[Ljava/lang/Object;)I");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  // EmitEvent method
  midEmitEvent = env->GetMethodID(clsITimpl, "emitEvent", "(IJ)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  // RegisterCE method
  midRegisterCE = env->GetMethodID(clsITimpl, "registerCE", "(Ljava/lang/String;Ljava/lang/String;ZZLjava/lang/String;I[Ljava/lang/Object;)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  // openFile method
  midOpenFile = env->GetMethodID(clsITimpl, "openFile", "(Ljava/lang/String;Lintegratedtoolkit/api/COMPSsRuntime$DataDirection;)Ljava/lang/String;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  // deleteFile method
  midDeleteFile = env->GetMethodID(clsITimpl, "deleteFile", "(Ljava/lang/String;)Z"); 
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  // waitForAllTasks method
  midWaitForAllTasks = env->GetMethodID(clsITimpl, "waitForAllTasks", "(Ljava/lang/Long;)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  // Parameter directions
  clsParDir = env->FindClass("integratedtoolkit/api/COMPSsRuntime$DataDirection");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midParDirCon = env->GetStaticMethodID(clsParDir, "valueOf", "(Ljava/lang/String;)Lintegratedtoolkit/api/COMPSsRuntime$DataDirection;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  jobjParDirIN =  env->CallStaticObjectMethod(clsParDir, midParDirCon, env->NewStringUTF("IN"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  jobjParDirINOUT = env->CallStaticObjectMethod(clsParDir, midParDirCon, env->NewStringUTF("INOUT"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  jobjParDirOUT =  env->CallStaticObjectMethod(clsParDir, midParDirCon, env->NewStringUTF("OUT"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  // Parameter classes
  clsObject = env->FindClass("java/lang/Object");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midObjCon = env->GetMethodID(clsObject, "<init>", "()V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  clsCharacter = env->FindClass("java/lang/Character");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midCharCon = env->GetMethodID(clsCharacter, "<init>", "(C)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  clsBoolean = env->FindClass("java/lang/Boolean");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midBoolCon = env->GetMethodID(clsBoolean, "<init>", "(Z)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  clsShort = env->FindClass("java/lang/Short");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midShortCon = env->GetMethodID(clsShort, "<init>", "(S)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  clsInteger = env->FindClass("java/lang/Integer");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midIntCon = env->GetMethodID(clsInteger, "<init>", "(I)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  clsLong = env->FindClass("java/lang/Long");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midLongCon = env->GetMethodID(clsLong, "<init>", "(J)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  clsFloat = env->FindClass("java/lang/Float");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midFloatCon = env->GetMethodID(clsFloat, "<init>", "(F)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  clsDouble = env->FindClass("java/lang/Double");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  midDoubleCon = env->GetMethodID(clsDouble, "<init>", "(D)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
}


void process_param(void **params, int i, jobjectArray jobjOBJArr) {
  int pv = i, pt = i + 1, pd = i + 2;
  
  void *parVal = params[pv];
  int parType = *(int*)params[pt];
  int parDirect = *(int*)params[pd];
  
  debug_printf("[   BINDING]  -  @process_param\n");
  
  jclass clsParType = NULL; /* integratedtoolkit.api.COMPSsRuntime$DataType class */
  clsParType = env->FindClass("integratedtoolkit/api/COMPSsRuntime$DataType");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  jmethodID midParTypeCon = NULL; /* ID of the integratedtoolkit.api.COMPSsRuntime$DataType class constructor method */
  midParTypeCon = env->GetStaticMethodID(clsParType, "valueOf", "(Ljava/lang/String;)Lintegratedtoolkit/api/COMPSsRuntime$DataType;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  jobject jobjParType = NULL;
  jobject jobjParVal = NULL;
  
  debug_printf ("[   BINDING]  -  @process_param  -  ENUM DT: %d\n", (enum datatype) parType);
  
  switch ( (enum datatype) parType) {
    case char_dt:
    case wchar_dt:
      jobjParVal = env->NewObject(clsCharacter, midCharCon, (jchar)*(char*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("CHAR_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case boolean_dt:
      jobjParVal = env->NewObject(clsBoolean, midBoolCon, (jboolean)*(int*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      debug_printf ("[   BINDING]  -  @process_param  -  Bool: %d\n", *(int*)parVal);
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("BOOLEAN_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case short_dt:
      jobjParVal = env->NewObject(clsShort, midShortCon, (jshort)*(short*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("SHORT_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case int_dt:
      jobjParVal = env->NewObject(clsInteger, midIntCon, (jint)*(int*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      debug_printf ("[   BINDING]  -  @process_param  -  Int: %d\n", *(int*)parVal);
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("INT_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case long_dt:
      jobjParVal = env->NewObject(clsLong, midLongCon, (jlong)*(long*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      debug_printf ("[   BINDING]  -  @process_param  -  Long: %ld\n", *(long*)parVal);
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("LONG_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case longlong_dt:
    case float_dt:
      jobjParVal = env->NewObject(clsFloat, midFloatCon, (jfloat)*(float*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      debug_printf ("[   BINDING]  -  @process_param  -  Float: %f\n", *(float*)parVal);
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("FLOAT_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case double_dt:
      jobjParVal = env->NewObject(clsDouble, midDoubleCon, (jdouble)*(double*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      debug_printf ("[   BINDING]  -  @process_param  -  Double: %f\n", *(double*)parVal);
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("DOUBLE_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case file_dt:
      jobjParVal = env->NewStringUTF(*(char **)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      debug_printf ("[   BINDING]  -  @process_param  -  File: %s\n", *(char **)parVal);
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("FILE_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case external_psco_dt:
      jobjParVal = env->NewStringUTF(*(char **)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      debug_printf ("[   BINDING]  -  @process_param  -  Persistent: %s\n", *(char **)parVal);
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("PSCO_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;                
    case string_dt:
      jobjParVal = env->NewStringUTF(*(char **)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      
      debug_printf ("[   BINDING]  -  @process_param  -  String: %s\n", *(char **)parVal);
      
      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("STRING_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
      break;
    case void_dt:
    case any_dt:
    case null_dt:
    default:
      break;
  }
  
  env->SetObjectArrayElement(jobjOBJArr, pv, jobjParVal);
  env->SetObjectArrayElement(jobjOBJArr, pt, jobjParType);
  
  debug_printf ("[   BINDING]  -  @process_param  -  ENUM DC: %d\n", (enum direction) parDirect);
  
  switch ((enum direction) parDirect) {
    case in_dir:
      env->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirIN);
      break;
    case out_dir:
      env->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirOUT);
      break;
    case inout_dir:
      env->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirINOUT);
      break;
    default:
      break;
  }
  
}

// API functions

void GS_On()
{
  debug_printf ("[   BINDING]  -  @GS_On\n");
  
  clsITimpl = NULL;
  jmethodID midITImplConst = NULL;
  jmethodID midStartIT = NULL;
  
  env = create_vm(&jvm);
  if (env == NULL) {
    printf ("[   BINDING]  -  @GS_On  -  Error creating the JVM\n");
    exit(1);
  }
  
  //Obtaining Classes
  clsITimpl = env->FindClass("integratedtoolkit/api/impl/COMPSsRuntimeImpl");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    printf("[   BINDING]  -  @GS_On  -  Error looking for the COMPSsRuntimeImpl class\n");
  }
  
  if (clsITimpl != NULL) {
    //Get constructor ID for COMPSsRuntimeImpl
    midITImplConst = env->GetMethodID(clsITimpl, "<init>", "()V");
    if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      printf("[   BINDING]  -  @GS_On  -  Error looking for the init method\n");
      exit(0);
    }
    
    midStartIT = env->GetMethodID(clsITimpl, "startIT", "()V");
    
    if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      printf("[   BINDING]  -  @GS_On  -  Error looking for the startIT method\n");
      exit(0);
    }
  } else {
    printf("[   BINDING]  -  @GS_On  -  Unable to find the requested class\n");
  }
  
  /************************************************************************/
  /* Now we will call the functions using the their method IDs            */
  /************************************************************************/
  
  if (midITImplConst != NULL) {
    if (clsITimpl != NULL && midITImplConst != NULL) {
      //Creating the Object of IT.
      jobjIT = env->NewObject(clsITimpl, midITImplConst);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
    }
    
    if (jobjIT != NULL && midStartIT != NULL) {
      env->CallVoidMethod(jobjIT, midStartIT); //Calling the method and passing IT Object as parameter
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(0);
      }
    } else
      printf("[   BINDING]  -  @GS_On  -  Unable to find the startit method\n");
    
  } else
    printf("[   BINDING]  -  @GS_On  -  Unable to find the requested method\n");
  
  init_jni_types();
  
  appId = env->NewObject(clsLong, midLongCon, (jlong) 0);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
}


void GS_Off()
{
  debug_printf("[   BINDING]  -  @GS_Off\n");
  
  jmethodID midStopIT = NULL;
  jmethodID midNoMoreTasksIT = NULL;
  
  midNoMoreTasksIT = env->GetMethodID(clsITimpl, "noMoreTasks", "(Ljava/lang/Long;Z)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  midStopIT = env->GetMethodID(clsITimpl, "stopIT", "(Z)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  env->CallVoidMethod(jobjIT, midNoMoreTasksIT, appId, "TRUE");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
  
  env->CallVoidMethod(jobjIT, midStopIT, "TRUE"); //Calling the method and passing IT Object as parameter
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
}

void GS_Get_AppDir(char **buf)
{
  debug_printf ("[   BINDING]  -  @GS_Get_AppDir\n");
  
  const char *cstr;
  jstring jstr = NULL;
  jboolean isCopy;
  
  jstr = (jstring)env->CallObjectMethod(jobjIT, midAppDir);
  
  debug_printf ("[   BINDING]  -  @GS_GetStringUTFChars\n");
  
  cstr = env->GetStringUTFChars(jstr, &isCopy);
  *buf = strdup(cstr);
  env->ReleaseStringUTFChars(jstr, cstr);
  
  debug_printf("[   BINDING]  -  @GS_Get_AppDir  -  directory name: %s\n", *buf);
}

void GS_ExecuteTask(long _appId, char *class_name, char *method_name, int priority, int has_target, int num_params, void **params)
{
  jobjectArray jobjOBJArr; /*  array of Objects to be passed to executeTask */
  
  debug_printf ("[   BINDING]  -  @GS_ExecuteTask\n");
  
  bool _priority = false;
  if (priority != 0) _priority = true;
  
  bool _has_target = false;
  if (has_target != 0) _has_target = true;
  
  jobjOBJArr = (jobjectArray)env->NewObjectArray(num_params*3, clsObject, env->NewObject(clsObject,midObjCon));
  
  for (int i=0; i<num_params*3; i+=3) {
    debug_printf("[   BINDING]  -  @GS_ExecuteTask  -  Processing pos %d\n", i);
    process_param(params, i, jobjOBJArr);
  }
  
  env->CallVoidMethod(jobjIT, midExecute, appId, env->NewStringUTF(class_name), env->NewStringUTF(method_name), _priority, _has_target, num_params, jobjOBJArr);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
}

void GS_RegisterCE(long _appId, char *class_name, char *method_name, int has_target, int has_return, int num_params, void **params, char *constraints)
{
  jobjectArray jobjOBJArr; /*  array of Objects to be passed to executeTask */
  
  debug_printf ("[   BINDING]  -  @GS_RegisterCE\n");
  
  bool _has_target = false;
  if (has_target != 0) _has_target = true;
  
  bool _has_return = false;
  if (has_return != 0) _has_return = true;
  
  jobjOBJArr = (jobjectArray)env->NewObjectArray(num_params*3, clsObject, env->NewObject(clsObject,midObjCon));
  
  for (int i = 0; i < num_params*3; i += 3) {
    debug_printf("[   BINDING]  -  @GS_RegisterCE  -  Processing pos %d\n", i);
    process_param(params, i, jobjOBJArr);
  }
  
  env->CallVoidMethod(jobjIT, midRegisterCE, env->NewStringUTF(class_name), env->NewStringUTF(method_name), _has_target, _has_return, env->NewStringUTF(constraints), num_params, jobjOBJArr);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(0);
  }
}

void GS_Get_File(char *file_name, int mode, char **buf)
{
  const char *cstr;
  jstring jstr = NULL;
  jboolean isCopy;
  
  switch ((enum direction) mode) {
    case in_dir:
      jstr = (jstring)env->CallObjectMethod(jobjIT, midOpenFile, env->NewStringUTF(file_name), jobjParDirIN);
      break;
    case out_dir:
      jstr = (jstring)env->CallObjectMethod(jobjIT, midOpenFile, env->NewStringUTF(file_name), jobjParDirOUT);
      break;
    case inout_dir:
      jstr = (jstring)env->CallObjectMethod(jobjIT, midOpenFile, env->NewStringUTF(file_name), jobjParDirINOUT);
      break;
    default:
      break;
  }
  
  cstr = env->GetStringUTFChars(jstr, &isCopy);
  *buf = strdup(cstr);
  env->ReleaseStringUTFChars(jstr, cstr);
  
  debug_printf("[   BINDING]  -  @GS_Get_File  -  COMPSs filename: %s\n", *buf);
}


void GS_Delete_File(char *file_name, int **buf)
{
  env->CallVoidMethod(jobjIT, midDeleteFile, env->NewStringUTF(file_name));
  if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      exit(0);
  }
  debug_printf("[   BINDING]  -  @GS_Delete_File  -  COMPSs filename: %s\n", *file_name);
}


void GS_WaitForAllTasks(long _appId)
{
  env->CallVoidMethod(jobjIT, midWaitForAllTasks, appId);
  if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      exit(0);
  }
  debug_printf("[   BINDING]  -  @GS_WaitForAllTasks  -  APP id: %lu", appId);
}


void GS_EmitEvent(int type, long id)
{
  if ( (type < 0 ) or (id < 0) ) {
    debug_printf ("[   BINDING]  -  @GS_EmitEvent  -  Error: event type and ID must be positive integers, but found: type: %u, ID: %lu\n", type, id);
  } else {
    debug_printf ("[   BINDING]  -  @GS_EmitEvent  -  Type: %u, ID: %lu\n", type, id);
    env->CallVoidMethod(jobjIT, midEmitEvent, type, id);
    if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      exit(0);
    }
  }
}
