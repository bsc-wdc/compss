#include <stdlib.h>
#include <jni.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>

#include "GS_compss.h"
#include "param_metadata.h"

// Uncomment the following define to get debug information.
//#define DEBUG_BINDING

#ifdef DEBUG_BINDING
#define debug_printf(args...) printf(args); fflush(stdout);
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
jmethodID midExecuteNew;	/* ID of the executeTask method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */
jmethodID midRegisterCE; 	/* ID of the RegisterCE method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */
jmethodID midEmitEvent; 	/* ID of the EmitEvent method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */

jmethodID midOpenFile; 		/* ID of the openFile method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */
jmethodID midDeleteFile; 	/* ID of the deleteFile method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */

jmethodID midBarrier; 		/* ID of the barrier method in the integratedtoolkit.api.impl.COMPSsRuntimeImpl class  */

jobject jobjParDirIN; 		/* Instance of the integratedtoolkit.types.annotations.parameter.Direction class */
jobject jobjParDirINOUT; 	/* Instance of the integratedtoolkit.types.annotations.parameter.Direction class */
jobject jobjParDirOUT; 		/* Instance of the integratedtoolkit.types.annotations.parameter.Direction class */

jobject jobjParStreamSTDIN;     /* Instance of the integratedtoolkit.types.annotations.parameter.Stream class */
jobject jobjParStreamSTDOUT;    /* Instance of the integratedtoolkit.types.annotations.parameter.Stream class */
jobject jobjParStreamSTDERR;    /* Instance of the integratedtoolkit.types.annotations.parameter.Stream class */
jobject jobjParStreamUNSPECIFIED; /* Instance of the integratedtoolkit.types.annotations.parameter.Stream class */

jstring jobjParPrefixEMPTY;     /* Instance of the integratedtoolkit.types.annotations.Constants.PREFIX_EMPTY */

jclass clsObject; 		/*  java.lang.Object class */
jmethodID midObjCon; 		/* ID of the java.lang.Object class constructor method */

jclass clsString;               /*  java.lang.String class */
jmethodID midStrCon;            /* ID of the java.lang.String class constructor method */

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


// *******************************
// Private functions
// *******************************

JNIEnv* create_vm(JavaVM ** jvm) {
  JNIEnv *env;
  JavaVMInitArgs vm_args;
  vector<JavaVMOption> options;

  string line; // buffer for line read
  const char *file = strdup(getenv("JVM_OPTIONS_FILE")); // path to the file with jvm options
  ifstream fin; // input file stream

  fin.open(file);
  if (fin.good()) {
    while (!fin.eof()) {
      // read in one line at a time
      getline(fin, line);
      // read data from file
      string fileOption = strdup(line.data());
      if (fileOption != "") {
        JavaVMOption *option = new JavaVMOption();
        int begin;
        int end;
        while ((begin = fileOption.find("$")) != fileOption.npos) {
          // It refers to an environment variable
          end = fileOption.find(":", begin);
          if (end == fileOption.npos) {
            end = fileOption.find("/", begin);
          }
          string prefix = fileOption.substr(0, begin);
          string env_varName = fileOption.substr(begin + 1, end - begin - 1);

          char *buffer = getenv(env_varName.data());
          if (buffer == NULL) {
            debug_printf("[   BINDING]  -  @create_vm  -  Cannot find environment variable: %s\n", env_varName.data());
          }

          string env_varValue(buffer);
          string suffix = "";
          if (end != fileOption.npos) {
              suffix = fileOption.substr(end);
          }
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
    exit(1);
  } else {
    debug_printf("[   BINDING]  -  @create_vm  -  JVM Ready\n");
  }
  return env;
}

void destroy_vm(JavaVM * jvm) {
  int ret = jvm->DestroyJavaVM();
  if (ret < 0){
    debug_printf("[   BINDING]  -  @destroy_vm  -  Unable to Destroy JVM - %i\n", ret);
  }
}

void init_jni_types() {
  jclass clsParDir; 		/* integratedtoolkit.types.annotations.parameter.Direction class */
  jmethodID midParDirCon; 	/* ID of the integratedtoolkit.types.annotations.parameter.Direction class constructor method */
  jclass clsParStream;          /* integratedtoolkit.types.annotations.parameter.Stream class */
  jmethodID midParStreamCon;    /* integratedtoolkit.types.annotations.parameter.Stream class constructor method */

  debug_printf ("[   BINDING]  -  @Init JNI Methods\n");

  // getApplicationDirectory method
  midAppDir = env->GetMethodID(clsITimpl, "getApplicationDirectory", "()Ljava/lang/String;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // executeTask method - Deprecated
  midExecute = env->GetMethodID(clsITimpl, "executeTask", "(Ljava/lang/Long;Ljava/lang/String;Ljava/lang/String;ZZI[Ljava/lang/Object;)I");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // executeTask New method
  midExecuteNew = env->GetMethodID(clsITimpl, "executeTask", "(Ljava/lang/Long;Ljava/lang/String;ZIZZZI[Ljava/lang/Object;)I");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // EmitEvent method
  midEmitEvent = env->GetMethodID(clsITimpl, "emitEvent", "(IJ)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // RegisterCE method
  midRegisterCE = env->GetMethodID(clsITimpl, "registerCoreElement", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // openFile method
  midOpenFile = env->GetMethodID(clsITimpl, "openFile", "(Ljava/lang/String;Lintegratedtoolkit/types/annotations/parameter/Direction;)Ljava/lang/String;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // deleteFile method
  midDeleteFile = env->GetMethodID(clsITimpl, "deleteFile", "(Ljava/lang/String;)Z");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // barrier method
  midBarrier = env->GetMethodID(clsITimpl, "barrier", "(Ljava/lang/Long;)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // Parameter directions

  debug_printf ("[   BINDING]  -  @Init JNI Direction Types\n");

  clsParDir = env->FindClass("integratedtoolkit/types/annotations/parameter/Direction");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midParDirCon = env->GetStaticMethodID(clsParDir, "valueOf", "(Ljava/lang/String;)Lintegratedtoolkit/types/annotations/parameter/Direction;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  jobjParDirIN =  env->CallStaticObjectMethod(clsParDir, midParDirCon, env->NewStringUTF("IN"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  jobjParDirINOUT = env->CallStaticObjectMethod(clsParDir, midParDirCon, env->NewStringUTF("INOUT"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  jobjParDirOUT =  env->CallStaticObjectMethod(clsParDir, midParDirCon, env->NewStringUTF("OUT"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // Parameter streams

  debug_printf ("[   BINDING]  -  @Init JNI Stream Types\n");

  clsParStream = env->FindClass("integratedtoolkit/types/annotations/parameter/Stream");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midParStreamCon = env->GetStaticMethodID(clsParStream, "valueOf", "(Ljava/lang/String;)Lintegratedtoolkit/types/annotations/parameter/Stream;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  jobjParStreamSTDIN =  env->CallStaticObjectMethod(clsParStream, midParStreamCon, env->NewStringUTF("STDIN"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  jobjParStreamSTDOUT =  env->CallStaticObjectMethod(clsParStream, midParStreamCon, env->NewStringUTF("STDOUT"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  jobjParStreamSTDERR =  env->CallStaticObjectMethod(clsParStream, midParStreamCon, env->NewStringUTF("STDERR"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  jobjParStreamUNSPECIFIED =  env->CallStaticObjectMethod(clsParStream, midParStreamCon, env->NewStringUTF("UNSPECIFIED"));
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  // Parameter prefix empty
  jobjParPrefixEMPTY = env->NewStringUTF("null");

  // Parameter classes

  debug_printf ("[   BINDING]  -  @Init JNI Types\n");

  clsObject = env->FindClass("java/lang/Object");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midObjCon = env->GetMethodID(clsObject, "<init>", "()V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  clsString = env->FindClass("java/lang/String");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midStrCon = env->GetMethodID(clsString, "<init>", "(Ljava/lang/String;)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  clsCharacter = env->FindClass("java/lang/Character");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midCharCon = env->GetMethodID(clsCharacter, "<init>", "(C)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  clsBoolean = env->FindClass("java/lang/Boolean");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midBoolCon = env->GetMethodID(clsBoolean, "<init>", "(Z)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  clsShort = env->FindClass("java/lang/Short");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midShortCon = env->GetMethodID(clsShort, "<init>", "(S)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  clsInteger = env->FindClass("java/lang/Integer");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midIntCon = env->GetMethodID(clsInteger, "<init>", "(I)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  clsLong = env->FindClass("java/lang/Long");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midLongCon = env->GetMethodID(clsLong, "<init>", "(J)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  clsFloat = env->FindClass("java/lang/Float");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midFloatCon = env->GetMethodID(clsFloat, "<init>", "(F)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  clsDouble = env->FindClass("java/lang/Double");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
  midDoubleCon = env->GetMethodID(clsDouble, "<init>", "(D)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  debug_printf ("[   BINDING]  -  @Init DONE\n");
}


void process_param(void **params, int i, jobjectArray jobjOBJArr) {
  // params is of the form:     value type direction
  // jobjOBJArr is of the form: value type direction stream prefix

  debug_printf("[   BINDING]  -  @process_param\n");

  void *parVal = params[3*i];
  int parType = *(int*)params[3*i + 1];
  int parDirect = *(int*)params[3*i + 2];

  int pv = 5*i, pt = 5*i + 1, pd = 5*i + 2, ps = 5*i + 3, pp = 5*i + 4;

  jclass clsParType = NULL; /* integratedtoolkit.types.annotations.parameter.DataType class */
  clsParType = env->FindClass("integratedtoolkit/types/annotations/parameter/DataType");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  jmethodID midParTypeCon = NULL; /* ID of the integratedtoolkit.api.COMPSsRuntime$DataType class constructor method */
  midParTypeCon = env->GetStaticMethodID(clsParType, "valueOf", "(Ljava/lang/String;)Lintegratedtoolkit/types/annotations/parameter/DataType;");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  jobject jobjParType = NULL;
  jobject jobjParVal = NULL;

  debug_printf ("[   BINDING]  -  @process_param  -  ENUM DATA_TYPE: %d\n", (enum datatype) parType);

  switch ( (enum datatype) parType) {
    case char_dt:
    case wchar_dt:
      jobjParVal = env->NewObject(clsCharacter, midCharCon, (jchar)*(char*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  Char: %c\n", *(char*)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("CHAR_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case boolean_dt:
      jobjParVal = env->NewObject(clsBoolean, midBoolCon, (jboolean)*(int*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  Bool: %d\n", *(int*)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("BOOLEAN_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case short_dt:
      jobjParVal = env->NewObject(clsShort, midShortCon, (jshort)*(short*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  Short: %hu\n", *(short*)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("SHORT_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case int_dt:
      jobjParVal = env->NewObject(clsInteger, midIntCon, (jint)*(int*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  Int: %d\n", *(int*)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("INT_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case long_dt:
      jobjParVal = env->NewObject(clsLong, midLongCon, (jlong)*(long*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  Long: %ld\n", *(long*)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("LONG_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case longlong_dt:
    case float_dt:
      jobjParVal = env->NewObject(clsFloat, midFloatCon, (jfloat)*(float*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  Float: %f\n", *(float*)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("FLOAT_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case double_dt:
      jobjParVal = env->NewObject(clsDouble, midDoubleCon, (jdouble)*(double*)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  Double: %f\n", *(double*)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("DOUBLE_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case file_dt:
      jobjParVal = env->NewStringUTF(*(char **)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  File: %s\n", *(char **)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("FILE_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case external_psco_dt:
      jobjParVal = env->NewStringUTF(*(char **)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      debug_printf ("[   BINDING]  -  @process_param  -  Persistent: %s\n", *(char **)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("EXTERNAL_PSCO_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case string_dt:
      jobjParVal = env->NewStringUTF(*(char **)parVal);
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }

      debug_printf ("[   BINDING]  -  @process_param  -  String: %s\n", *(char **)parVal);

      jobjParType = env->CallStaticObjectMethod(clsParType, midParTypeCon, env->NewStringUTF("STRING_T"));
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
      break;
    case void_dt:
    case any_dt:
    case null_dt:
    default:
      break;
  }

  // Sets the parameter value and type
  env->SetObjectArrayElement(jobjOBJArr, pv, jobjParVal);
  env->SetObjectArrayElement(jobjOBJArr, pt, jobjParType);

  // Add param direction
  debug_printf ("[   BINDING]  -  @process_param  -  ENUM DIRECTION: %d\n", (enum direction) parDirect);
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

  // Add param stream
  env->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamUNSPECIFIED);

  // Add param prefix
  env->SetObjectArrayElement(jobjOBJArr, pp, jobjParPrefixEMPTY);
}

// ******************************
// API functions
// ******************************

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
    exit(1);
  }

  if (clsITimpl != NULL) {
    //Get constructor ID for COMPSsRuntimeImpl
    midITImplConst = env->GetMethodID(clsITimpl, "<init>", "()V");
    if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      printf("[   BINDING]  -  @GS_On  -  Error looking for the init method\n");
      exit(1);
    }

    midStartIT = env->GetMethodID(clsITimpl, "startIT", "()V");

    if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      printf("[   BINDING]  -  @GS_On  -  Error looking for the startIT method\n");
      exit(1);
    }
  } else {
    printf("[   BINDING]  -  @GS_On  -  Unable to find the requested class\n");
    exit(1);
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
	exit(1);
      }
    }

    if (jobjIT != NULL && midStartIT != NULL) {
      env->CallVoidMethod(jobjIT, midStartIT); //Calling the method and passing IT Object as parameter
      if (env->ExceptionOccurred()) {
	env->ExceptionDescribe();
	exit(1);
      }
    } else {
      printf("[   BINDING]  -  @GS_On  -  Unable to find the startit method\n");
      exit(1);
    }
  } else {
    printf("[   BINDING]  -  @GS_On  -  Unable to find the requested method\n");
    exit(1);
  }

  init_jni_types();

  appId = env->NewObject(clsLong, midLongCon, (jlong) 0);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
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
    exit(1);
  }

  midStopIT = env->GetMethodID(clsITimpl, "stopIT", "(Z)V");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  env->CallVoidMethod(jobjIT, midNoMoreTasksIT, appId, "TRUE");
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  env->CallVoidMethod(jobjIT, midStopIT, "TRUE"); //Calling the method and passing IT Object as parameter
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

  destroy_vm(jvm);  // Release jvm resources -- Does not work properly --> JNI bug: not releasing properly the resources, so it is not possible to recreate de JVM.
  // delete jvm;    // free(): invalid pointer: 0x00007fbc11ba8020 ***
  jvm = NULL;
}

void GS_Get_AppDir(char **buf)
{
  debug_printf ("[   BINDING]  -  @GS_Get_AppDir\n");

  const char *cstr;
  jstring jstr = NULL;
  jboolean isCopy;

  jstr = (jstring)env->CallObjectMethod(jobjIT, midAppDir);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }

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

  jobjOBJArr = (jobjectArray)env->NewObjectArray(num_params*5, clsObject, env->NewObject(clsObject,midObjCon));

  for (int i = 0; i < num_params; i++) {
    debug_printf("[   BINDING]  -  @GS_ExecuteTask  -  Processing pos %d\n", i);
    process_param(params, i, jobjOBJArr);
  }

  env->CallVoidMethod(jobjIT, midExecute, appId, env->NewStringUTF(class_name), env->NewStringUTF(method_name), _priority, _has_target, num_params, jobjOBJArr);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
}

void GS_ExecuteTaskNew(long _appId, char *signature, int priority, int num_nodes, int replicated, int distributed, int has_target, int num_params, void **params)
{
  jobjectArray jobjOBJArr; /* array of Objects to be passed to executeTask */

  debug_printf ("[   BINDING]  -  @GS_ExecuteTaskNew\n");

  bool _priority = false;
  if (priority != 0) _priority = true;

  bool _replicated = false;
  if (replicated != 0) _replicated = true;

  bool _distributed = false;
  if (distributed != 0) _distributed = true;

  bool _has_target = false;
  if (has_target != 0) _has_target = true;

  jobjOBJArr = (jobjectArray)env->NewObjectArray(num_params*5, clsObject, env->NewObject(clsObject,midObjCon));

  for (int i = 0; i < num_params; i++) {
    debug_printf("[   BINDING]  -  @GS_ExecuteTask  -  Processing pos %d\n", i);
    process_param(params, i, jobjOBJArr);
  }

  env->CallVoidMethod(jobjIT, midExecuteNew, appId,
                                             env->NewStringUTF(signature),
                                             _priority,
                                             num_nodes,
                                             _replicated,
                                             _distributed,
                                             _has_target,
                                             num_params,
                                             jobjOBJArr);
  if (env->ExceptionOccurred()) {
    env->ExceptionDescribe();
    exit(1);
  }
}

void GS_RegisterCE(char *CESignature, char *ImplSignature, char *ImplConstraints, char *ImplType, int num_params, char **ImplTypeArgs)
{
  debug_printf ("[   BINDING]  -  @GS_RegisterCE\n");
  //debug_printf ("[   BINDING]  -  @GS_RegisterCE - CESignature:     %s\n", CESignature);
  //debug_printf ("[   BINDING]  -  @GS_RegisterCE - ImplSignature:   %s\n", ImplSignature);
  //debug_printf ("[   BINDING]  -  @GS_RegisterCE - ImplConstraints: %s\n", ImplConstraints);
  //debug_printf ("[   BINDING]  -  @GS_RegisterCE - ImplType:        %s\n", ImplType);
  //debug_printf ("[   BINDING]  -  @GS_RegisterCE - num_params:      %d\n", num_params);

  jobjectArray implArgs; //  array of Objects to be passed to register core element
  implArgs = (jobjectArray)env->NewObjectArray(num_params, clsString, env->NewStringUTF(""));
  for (int i = 0; i < num_params; i++) {
    //debug_printf("[   BINDING]  -  @GS_RegisterCE  -    Processing pos %d\n", i);
    jstring tmp = env->NewStringUTF(ImplTypeArgs[i]);
    env->SetObjectArrayElement(implArgs, i, tmp);
  }

  env->CallVoidMethod(jobjIT, midRegisterCE, env->NewStringUTF(CESignature),
                                             env->NewStringUTF(ImplSignature),
                                             env->NewStringUTF(ImplConstraints),
                                             env->NewStringUTF(ImplType),
                                             implArgs);
  if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      exit(1);
  }
  debug_printf("[   BINDING]  -  @GS_RegisterCE  -  Task registered: %s\n", CESignature);
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

  if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      exit(1);
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
      exit(1);
  }
  debug_printf("[   BINDING]  -  @GS_Delete_File  -  COMPSs filename: %s\n", file_name);
}


void GS_Barrier(long _appId)
{
  env->CallVoidMethod(jobjIT, midBarrier, appId);
  if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      exit(1);
  }
  debug_printf("[   BINDING]  -  @GS_Barrier  -  APP id: %lu", appId);
}


void GS_EmitEvent(int type, long id)
{
  if ( (type < 0 ) or (id < 0) ) {
    debug_printf ("[   BINDING]  -  @GS_EmitEvent  -  Error: event type and ID must be positive integers, but found: type: %u, ID: %lu\n", type, id);
    exit(1);
  } else {
    debug_printf ("[   BINDING]  -  @GS_EmitEvent  -  Type: %u, ID: %lu\n", type, id);
    env->CallVoidMethod(jobjIT, midEmitEvent, type, id);
    if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
      exit(1);
    }
  }
}
