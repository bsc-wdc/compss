/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

#include <stdlib.h>
#include <jni.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>

#include "compss_interface.h"
#include "compss_jni.h"
#include "param_metadata.h"
#include "BindingDataManager.h"

using namespace std;

typedef struct {
  int isLocked;
  JNIEnv* localJniEnv;
  JavaVM* localJvm;
  int isAttached;
} ThreadStatus;


typedef struct JNIWorkflow {
    CompssWorkflow base;
    jobject jWorkflow;
} JNIWorkflow;

JNIEnv* globalJniEnv;
JavaVM* globalJvm;
pthread_mutex_t globalJniAccessMutex;
jobject globalRuntime;

CompssWorkflow* JNI_wf;
long JNI_wf_appId;

jmethodID midStopIT;                    /* ID of the stopIT method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midAppDir;                    /* ID of the getApplicationDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midTempDir;                   /* ID of the getTempDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midRegWf;                     /* ID of the registerWorkflow method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jclass clsWorkflow;                     /* Class implementing the Workflow interface at runtime */
jmethodID mid_wf_getID;                 /* ID of the getID method in the Class implementing the Workflow interface*/
jmethodID mid_wf_openTaskGroup;
jmethodID mid_wf_closeTaskGroup;
jmethodID mid_wf_cancelTaskGroup;
jmethodID mid_wf_cancelApplicationTasks;
jmethodID mid_wf_noMoreTasks;
jmethodID mid_wf_barrier;
jmethodID mid_wf_barrier_withFlag;
jmethodID mid_wf_barrierGroup;
jmethodID mid_wf_snapshot;
jmethodID mid_wf_deregister;
jmethodID mid_wf_getBindingObject;		
jmethodID mid_wf_deleteBindingObject; 	

jmethodID midExecute;                   /* ID of the executeTask method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midExecuteNew;                /* ID of the executeTask method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midExecuteHttp;                /* ID of the executeTask method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midRegisterCE;                /* ID of the RegisterCE method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midEmitEvent;                 /* ID of the EmitEvent method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midIsFileAccessed;            /* ID of the isFileAccessed method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midOpenFile;                  /* ID of the openFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midCloseFile;                 /* ID of the closeFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midDeleteFile;                /* ID of the deleteFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midGetFile;                   /* ID of the getFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midGetDirectory;              /* ID of the getDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */



jmethodID midGetNumberOfResources;      /* ID of the getNumberOfResources method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midRequestResources;          /* ID of the requestResources method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midFreeResources;             /* ID of the freeResources method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midSetWallClockLimit;			/* ID of the setWallClockLimit method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jclass clsOnFailure;
jmethodID midOnFailureCon;

typedef struct {                        /* Instances of the es.bsc.compss.types.annotations.parameter.DataType class */
    jobject CHAR_T;
    jobject BOOLEAN_T;
    jobject SHORT_T;
    jobject INT_T;
    jobject LONG_T;
    jobject FLOAT_T;
    jobject DOUBLE_T;
    jobject FILE_T;
    jobject DIRECTORY_T;
    jobject EXTERNAL_STREAM_T;
    jobject EXTERNAL_PSCO_T;
    jobject STRING_T;
    jobject STRING_64_T;
    jobject BINDING_OBJECT_T;
    jobject COLLECTION_T;
    jobject DICT_COLLECTION_T;
    jobject NULL_T;
}ParamType;
ParamType par_type;

typedef struct {                        /* Instances of the es.bsc.compss.types.annotations.parameter.Direction class */
    jobject IN;
    jobject IN_DELETE;
    jobject OUT;
    jobject INOUT;
    jobject CONCURRENT;
    jobject COMMUTATIVE;
} ParamDirections;
ParamDirections par_dir;

typedef struct {                        /* Instances of the es.bsc.compss.types.annotations.parameter.StdIOStream class */
    jobject STDIN;
    jobject STDOUT;
    jobject STDERR;
    jobject UNSPECIFIED;
} StdStream;
StdStream std_stream;

jstring jobjParPrefixEMPTY;         /* Instance of the es.bsc.compss.types.annotations.Constants.PREFIX_EMPTY */

jclass clsObject;         /* java.lang.Object class */
jmethodID midObjCon;      /* ID of the java.lang.Object class constructor method */

jclass clsString;         /* java.lang.String class */
jmethodID midStrCon;      /* ID of the java.lang.String class constructor method */

jclass clsCharacter;      /* java.lang.Character class */
jmethodID midCharCon;     /* ID of the java.lang.Character class constructor method */

jclass clsBoolean;        /* java.lang.Boolean class */
jmethodID midBoolCon;     /* ID of the java.lang.clsBoolean class constructor method */

jclass clsShort;          /* java.lang.Short class */
jmethodID midShortCon;    /* ID of the java.lang.Short class constructor method */

jclass clsInteger;        /* java.lang.Integer class */
jmethodID midIntCon;      /* ID of the java.lang.Integer class constructor method */

jclass clsLong;           /* java.lang.Long class */
jmethodID midLongCon;     /* ID of the java.lang.Long class constructor method */
jmethodID midLongVal;     /* ID of the java.lang.Long class longValue method */

jclass clsFloat;          /* java.lang.Float class */
jmethodID midFloatCon;    /* ID of the java.lang.Float class constructor method */

jclass clsDouble;         /* java.lang.Double class */
jmethodID midDoubleCon;   /* ID of the java.lang.Double class constructor method */


// ******************************
// Private helper methods
// ******************************

/**
 * Registers and attaches the current thread to access the JVM.
 * Requires globalJniAccessMutex, globalJniEnv, and globalJvm to be initialised.
 */
ThreadStatus* access_request() {
    ThreadStatus* status = new ThreadStatus();

    // Lock mutex
    pthread_mutex_lock(&globalJniAccessMutex);
    status->isLocked = 1;

    // Attach thread to JVM
    status->localJniEnv = globalJniEnv;
    status->localJvm = globalJvm;
    status->isAttached = check_and_attach(globalJvm, status->localJniEnv); // WARN: Updates isAttached and localEnv

    // Return status
    return status;
}

/**
 * Revokes the current thread to access the JVM. The given status cannot be user after this callee.
 */
void access_revoke(ThreadStatus* status) {
    // Detach thread from JVM
    if (status->localJvm != NULL && status->isAttached == 1) {
        status->localJvm->DetachCurrentThread();

        status->localJniEnv = NULL;
        status->localJvm = NULL;
        status->isAttached = 0;
    }

    // Unlock mutex
    if (status->isLocked == 1) {
        pthread_mutex_unlock(&globalJniAccessMutex);

        status->isLocked = 0;
    }

    // Free status memory
    free(status);
}

/**
 * Checks if an exception occurred. If an exception is registered, log it, revoke the
 * thread JVM permissions, and exit.
 */
void check_exception(ThreadStatus* status, const char* message) {
    if (status->localJniEnv->ExceptionOccurred()) {
        // Log provided exception message
        print_error("\n[BINDING-COMMONS] ERROR: %s. \n", message);

        // Log exception
        status->localJniEnv->ExceptionDescribe();

        // Free thread status
        access_revoke(status);

        // Error Exit
        exit(1);
    }
}

/**
 * Gets the containing message of a given a COMPSsException. Does not stop the execution.
 */
void check_and_get_compss_exception(ThreadStatus* status, char** buf) {
    jthrowable exception = status->localJniEnv->ExceptionOccurred();
    if (exception) {
        // Log exception
        status->localJniEnv->ExceptionDescribe();

        // Parse COMPSsException
        jclass exceptionClazz = status->localJniEnv->GetObjectClass((jobject) exception);
        jclass classClazz = status->localJniEnv->GetObjectClass((jobject) exceptionClazz);
        jmethodID classGetNameMethod = status->localJniEnv->GetMethodID(classClazz, "getName", "()Ljava/lang/String;");
        jstring classNameStr = (jstring)status->localJniEnv->CallObjectMethod(exceptionClazz, classGetNameMethod);
        if (strcmp(status->localJniEnv->GetStringUTFChars(classNameStr ,0), "es.bsc.compss.worker.COMPSsException") != 0) {
            status->localJniEnv->ExceptionClear();
        } else {
            const char* classNameChars = status->localJniEnv->GetStringUTFChars(classNameStr, NULL);
            if (classNameChars != NULL) {
                jmethodID throwableGetMessageMethod = status->localJniEnv->GetMethodID(exceptionClazz, "getMessage", "()Ljava/lang/String;");
                jstring messageStr = (jstring) status->localJniEnv->CallObjectMethod(exception, throwableGetMessageMethod);
                if (messageStr != NULL) {
                    const char* messageChars = status->localJniEnv->GetStringUTFChars( messageStr, NULL);
                    if (messageChars != NULL) {
                        *buf = strdup(messageChars);
                        status->localJniEnv->ReleaseStringUTFChars(messageStr, messageChars);
                        status->localJniEnv->ExceptionClear();
                    } else {
                        status->localJniEnv->ExceptionClear();
                    }
                    status->localJniEnv->DeleteLocalRef(messageStr);
                }
                status->localJniEnv->ReleaseStringUTFChars(classNameStr, classNameChars);
                status->localJniEnv->DeleteLocalRef(classNameStr);
            }
            status->localJniEnv->DeleteLocalRef(classClazz);
            status->localJniEnv->DeleteLocalRef(exceptionClazz);
        }
    }
}


void defineBasicType(ThreadStatus* status, const char* label, const char* name, const char* args, jclass* cls, jmethodID* midCon){
    char err_msg[256];
    snprintf(err_msg, 256, "Cannot find %s Class", label);
    jclass clsLocal = status->localJniEnv->FindClass(name);
    check_exception(status, err_msg);
    *cls = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, err_msg);
    *midCon = status->localJniEnv->GetMethodID(*cls, "<init>", args);
    check_exception(status, err_msg);
}

void defineBasicType(ThreadStatus* status, const char* label, const char* name, const char* args, const char* getVal,const char* getValArgs, jclass* cls, jmethodID* midCon, jmethodID* midVal){
    char err_msg[256];
    defineBasicType(status, label, name, args, cls, midCon);
    snprintf(err_msg, 256, "Cannot find %s Method", getVal);
    *midVal = status->localJniEnv->GetMethodID(*cls, getVal, getValArgs);
    check_exception(status, err_msg);
}
/**
 * Initialises the JNI basic types.
 */
void init_basic_jni_types(ThreadStatus* status) {
    // Parameter classes
    debug_printf ("[BINDING-COMMONS] - @Init JNI Types\n");
    defineBasicType(status, "Object", (char*)"java/lang/Object", (char*)"()V", &clsObject, &midObjCon);
    defineBasicType(status, (char*)"String", "java/lang/String", (char*)"(Ljava/lang/String;)V", &clsString, &midStrCon);
    defineBasicType(status, (char*)"Char", (char*)"java/lang/Character", "(C)V", &clsCharacter, &midCharCon);
    defineBasicType(status, (char*)"Boolean", (char*)"java/lang/Boolean", (char*)"(Z)V", &clsBoolean, &midBoolCon);
    defineBasicType(status, (char*)"Short", (char*)"java/lang/Short", (char*)"(S)V", &clsShort, &midShortCon);
    defineBasicType(status, (char*)"Integer", (char*)"java/lang/Integer", (char*)"(I)V", &clsInteger, &midIntCon);
    defineBasicType(status, (char*)"Long", (char*)"java/lang/Long", (char*)"(J)V", "longValue", "()J", &clsLong, &midLongCon, &midLongVal);
    defineBasicType(status, (char*)"Float", (char*)"java/lang/Float", (char*)"(F)V", &clsFloat, &midFloatCon);
    defineBasicType(status, (char*)"Double", (char*)"java/lang/Double", (char*)"(D)V", &clsDouble, &midDoubleCon);
    debug_printf ("[BINDING-COMMONS] - @Init JNI Types DONE\n");
}

jobject init_param_field(ThreadStatus* status, jclass clsParField, jmethodID midParFieldCon, const char* field, const char* value) {
    char err_msg[256];

    snprintf(err_msg, 256, "Cannot retrieve %s.%s object", field, value);
    jobject objLocal = status->localJniEnv->CallStaticObjectMethod(clsParField, midParFieldCon, status->localJniEnv->NewStringUTF(value));
    check_exception(status, err_msg);

    snprintf(err_msg, 256, "Cannot create global reference for %s.%s object", field, value);
    jobject jobjParType = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, err_msg);
    return jobjParType;
}


void init_param_types(ThreadStatus* status){

    jclass clsParType = NULL; /* es.bsc.compss.types.annotations.parameter.DataType class */
    clsParType = status->localJniEnv->FindClass("es/bsc/compss/types/annotations/parameter/DataType");
    check_exception(status, "Cannot load DataType class");

    jmethodID midParTypeCon = NULL; /* ID of the es.bsc.compss.api.COMPSsRuntime$DataType class constructor method */
    midParTypeCon = status->localJniEnv->GetStaticMethodID(clsParType, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/DataType;");
    check_exception(status, "Cannot get DataType constructor");

    par_type.CHAR_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "CHAR_T");
    par_type.BOOLEAN_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "BOOLEAN_T");
    par_type.SHORT_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "SHORT_T");
    par_type.INT_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "INT_T");
    par_type.LONG_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "LONG_T");
    par_type.FLOAT_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "FLOAT_T");
    par_type.DOUBLE_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "DOUBLE_T");
    par_type.FILE_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "FILE_T");
    par_type.DIRECTORY_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "DIRECTORY_T");
    par_type.EXTERNAL_STREAM_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "EXTERNAL_STREAM_T");
    par_type.EXTERNAL_PSCO_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "EXTERNAL_PSCO_T");
    par_type.STRING_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "STRING_T");
    par_type.STRING_64_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "STRING_64_T");
    par_type.BINDING_OBJECT_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "BINDING_OBJECT_T");
    par_type.COLLECTION_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "COLLECTION_T");
    par_type.DICT_COLLECTION_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "DICT_COLLECTION_T");
    par_type.NULL_T = init_param_field(status, clsParType, midParTypeCon, "DataType", "NULL_T");
}

void init_param_directions(ThreadStatus* status) {

    jclass clsParDir; 		    /* es.bsc.compss.types.annotations.parameter.Direction class */
    jmethodID midParDirCon; 	/* ID of the es.bsc.compss.types.annotations.parameter.Direction class constructor method */

    clsParDir = status->localJniEnv->FindClass("es/bsc/compss/types/annotations/parameter/Direction");
    check_exception(status, "Cannot find Direction Class");
    midParDirCon = status->localJniEnv->GetStaticMethodID(clsParDir, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/Direction;");
    check_exception(status, "Cannot find Direction constructor");

    par_dir.IN = init_param_field(status, clsParDir, midParDirCon, "Direction", "IN");
    par_dir.IN_DELETE = init_param_field(status, clsParDir, midParDirCon, "Direction", "IN_DELETE");
    par_dir.OUT = init_param_field(status, clsParDir, midParDirCon, "Direction", "OUT");
    par_dir.INOUT = init_param_field(status, clsParDir, midParDirCon, "Direction", "INOUT");
    par_dir.CONCURRENT = init_param_field(status, clsParDir, midParDirCon, "Direction", "CONCURRENT");
    par_dir.COMMUTATIVE = init_param_field(status, clsParDir, midParDirCon, "Direction", "COMMUTATIVE");
}

void init_std_streams(ThreadStatus* status) {
    jclass clsParStream;        /* es.bsc.compss.types.annotations.parameter.StdIOStream class */
    jmethodID midParStreamCon;  /* es.bsc.compss.types.annotations.parameter.StdIOStream class constructor method */

    clsParStream = status->localJniEnv->FindClass("es/bsc/compss/types/annotations/parameter/StdIOStream");
    check_exception(status, "Cannot find StdIOStream class");
    midParStreamCon = status->localJniEnv->GetStaticMethodID(clsParStream, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/StdIOStream;");
    check_exception(status, "Cannot find StdIOStream constructor");

    std_stream.STDIN = init_param_field(status, clsParStream, midParStreamCon, "StdIOStream", "STDIN");
    std_stream.STDOUT = init_param_field(status, clsParStream, midParStreamCon, "StdIOStream", "STDOUT");
    std_stream.STDERR = init_param_field(status, clsParStream, midParStreamCon, "StdIOStream", "STDERR");
    std_stream.UNSPECIFIED = init_param_field(status, clsParStream, midParStreamCon, "StdIOStream", "UNSPECIFIED");   
}
/**
 * Initialises the COMPSs related types.
 */
void init_master_jni_types(ThreadStatus* status, jclass clsITimpl) {
    debug_printf ("[BINDING-COMMONS] - @Init JNI Master\n");

    // JNI API method calls
    debug_printf ("[BINDING-COMMONS] - @Init JNI Methods\n");

    midRegWf = status->localJniEnv->GetMethodID(clsITimpl, "registerWorkflow", "(Ljava/lang/String;Les/bsc/compss/api/ApplicationRunner;)Les/bsc/compss/api/Workflow;");
    check_exception(status, "Cannot find registerWorkflow method");

    // getApplicationDirectory method
    midAppDir = status->localJniEnv->GetMethodID(clsITimpl, "getApplicationDirectory", "()Ljava/lang/String;");
    check_exception(status, "Cannot find getApplicationDirectory method");

    // getMasterWorkingDirectory method
    midTempDir = status->localJniEnv->GetMethodID(clsITimpl, "getTempDir", "()Ljava/lang/String;");
    check_exception(status, "Cannot find getMasterWorkingDirectory method");

    // executeTask method - C binding
    midExecute = status->localJniEnv->GetMethodID(clsITimpl, "executeTask", "(Ljava/lang/Long;Ljava/lang/String;Ljava/lang/String;ILjava/lang/String;ZIZIZZZLjava/lang/Integer;I[Ljava/lang/Object;)I");
    check_exception(status, "Cannot find executeTask C");

    // executeTask method - Python binding
    midExecuteNew = status->localJniEnv->GetMethodID(clsITimpl, "executeTask", "(Ljava/lang/Long;Ljava/lang/String;Ljava/lang/String;IZIZIZZZLjava/lang/Integer;I[Ljava/lang/Object;)I");
    check_exception(status, "Cannot find executeTask Python");

    // executeTask method - Http tasks
    midExecuteHttp = status->localJniEnv->GetMethodID(clsITimpl, "executeTask", "(Ljava/lang/Long;Ljava/lang/String;ZIZIZZZILes/bsc/compss/types/annotations/parameter/OnFailure;I[Ljava/lang/Object;)I");
    check_exception(status, "Cannot find executeTask HTTP");

    // EmitEvent method
    midEmitEvent = status->localJniEnv->GetMethodID(clsITimpl, "emitEvent", "(IJ)V");
    check_exception(status, "Cannot find emitEvent");

    // RegisterCE method
    midRegisterCE = status->localJniEnv->GetMethodID(clsITimpl, "registerCoreElement", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;)V");
    check_exception(status, "Cannot find registerCoreElement");

    // isFileAccessed method
    midIsFileAccessed = status->localJniEnv->GetMethodID(clsITimpl, "isFileAccessed", "(Ljava/lang/Long;Ljava/lang/String;)Z");
    check_exception(status, "Cannot find isFileAccessed");

    // openFile method
    midOpenFile = status->localJniEnv->GetMethodID(clsITimpl, "openFile", "(Ljava/lang/Long;Ljava/lang/String;Les/bsc/compss/types/annotations/parameter/Direction;)Ljava/lang/String;");
    check_exception(status, "Cannot find openFile");

    // closeFile method
    midCloseFile = status->localJniEnv->GetMethodID(clsITimpl, "closeFile", "(Ljava/lang/Long;Ljava/lang/String;Les/bsc/compss/types/annotations/parameter/Direction;)V");
    check_exception(status, "Cannot find closeFile");

    // deleteFile method
    midDeleteFile = status->localJniEnv->GetMethodID(clsITimpl, "deleteFile", "(Ljava/lang/Long;Ljava/lang/String;ZZ)Z");
    check_exception(status, "Cannot find deleteFile");

    // getFile method
    midGetFile = status->localJniEnv->GetMethodID(clsITimpl, "getFile", "(Ljava/lang/Long;Ljava/lang/String;)V");
    check_exception(status, "Cannot find getFile");

    // getDirectory method
    midGetDirectory = status->localJniEnv->GetMethodID(clsITimpl, "getDirectory", "(Ljava/lang/Long;Ljava/lang/String;)V");
    check_exception(status, "Cannot find getDirectory");

    // getNumberOfResources method
    midGetNumberOfResources = status->localJniEnv->GetMethodID(clsITimpl, "getNumberOfResources", "()I");
    check_exception(status, "Cannot find getNumberOfResources");

    // requestResourcesCreation method
    midRequestResources = status->localJniEnv->GetMethodID(clsITimpl, "requestResources", "(Ljava/lang/Long;ILjava/lang/String;)V");
    check_exception(status, "Cannot find requestResources");

    // requestResourcesDestruction method
    midFreeResources = status->localJniEnv->GetMethodID(clsITimpl, "freeResources", "(Ljava/lang/Long;ILjava/lang/String;)V");
    check_exception(status, "Cannot find freeResources");

    // Load stopIT
    midStopIT = status->localJniEnv->GetMethodID(clsITimpl, "stopIT", "(Z)V");
    check_exception(status, "Cannot find stopIT method.");

    // Load setWallClockLimit
    midSetWallClockLimit = status->localJniEnv->GetMethodID(clsITimpl, "setWallClockLimit", "(Ljava/lang/Long;JZ)V");
    check_exception(status, "Cannot find setWallClockLimit");


    debug_printf ("[BINDING-COMMONS] - @Init JNI Methods DONE\n");

    // Task OnFailure behaviour
    debug_printf ("[BINDING-COMMONS] - @Init JNI OnFailure Types\n");

    clsOnFailure = status->localJniEnv->FindClass("es/bsc/compss/types/annotations/parameter/OnFailure");
    check_exception(status, "Cannot find OnFailure Class");
    midOnFailureCon = status->localJniEnv->GetStaticMethodID(clsOnFailure, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/OnFailure;");
    check_exception(status, "Cannot find OnFailure constructor");

    // Parameter directions
    debug_printf ("[BINDING-COMMONS] - @Init JNI Parameter Types\n");
    init_param_types(status);
    debug_printf ("[BINDING-COMMONS] - @Init JNI Parameter Types DONE\n");

    // Parameter directions
    debug_printf ("[BINDING-COMMONS] - @Init JNI Direction Types\n");
    init_param_directions(status);
    debug_printf ("[BINDING-COMMONS] - @Init JNI Direction Types DONE\n");


    // Parameter streams
    debug_printf ("[BINDING-COMMONS] - @Init JNI Stream Types\n");
    init_std_streams(status);
    debug_printf ("[BINDING-COMMONS] - @Init JNI Stream Types\n");


    // Parameter prefix empty
    debug_printf ("[BINDING-COMMONS] - @Init JNI Parameter Prefix\n");

    jstring objStr = status->localJniEnv->NewStringUTF("null");
    check_exception(status, "Error getting null string object");
    jobjParPrefixEMPTY = (jstring)status->localJniEnv->NewGlobalRef(objStr);
    check_exception(status, "Error getting null string object");

    debug_printf ("[BINDING-COMMONS] - @Init JNI Parameter Prefix DONE\n");

    // Done
    debug_printf ("[BINDING-COMMONS] - @Init Master DONE\n");
}


/**
 * Processes the given parameter information.
 */
void process_param(ThreadStatus* status, void** params, int i, jobjectArray jobjOBJArr) {
    debug_printf("[BINDING-COMMONS] - @process_param\n");
    // params     is of the form: value type direction stream prefix name
    // jobjOBJArr is of the form: value type direction stream prefix name
    // This means that the ith parameters occupies the fields in the interval [NF * k, NK * k + 8]
    int pv = NUM_FIELDS * i + 0,
        pt = NUM_FIELDS * i + 1,
        pd = NUM_FIELDS * i + 2,
        ps = NUM_FIELDS * i + 3,
        pp = NUM_FIELDS * i + 4,
        pn = NUM_FIELDS * i + 5,
        pc = NUM_FIELDS * i + 6,
        pw = NUM_FIELDS * i + 7,
        pkr = NUM_FIELDS * i + 8;

    void *parVal        =           params[pv];
    int parType         = *(int*)   params[pt];
    int parDirect       = *(int*)   params[pd];
    int parIOStream     = *(int*)   params[ps];
    void *parPrefix     =           params[pp];
    void *parName       =           params[pn];
    void *parConType    =           params[pc];
    void *parWeight	    =           params[pw];
    int parKeepRename   = *(int*)   params[pkr];

    jobject jobjParType = NULL;
    jobject jobjParVal = NULL;

    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM DATA_TYPE: %d\n", (enum datatype) parType);

    switch ( (enum datatype) parType) {
        case char_dt:
        case wchar_dt:
            jobjParType = par_type.CHAR_T;
            jobjParVal = status->localJniEnv->NewObject(clsCharacter, midCharCon, (jchar)*(char*)parVal);
            check_exception(status, "Cannot instantiate new char object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Char: %c\n", *(char*)parVal);
            break;
        case boolean_dt:
            jobjParType = par_type.BOOLEAN_T;
            jobjParVal = status->localJniEnv->NewObject(clsBoolean, midBoolCon, (jboolean)*(int*)parVal);
            check_exception(status, "Cannot instantiate new boolean object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Bool: %d\n", *(int*)parVal);
            break;
        case short_dt:
            jobjParType = par_type.SHORT_T;
            jobjParVal = status->localJniEnv->NewObject(clsShort, midShortCon, (jshort)*(short*)parVal);
            check_exception(status, "Cannot instantiate new short object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Short: %hu\n", *(short*)parVal);
            break;
        case int_dt:
            jobjParType = par_type.INT_T;
            jobjParVal = status->localJniEnv->NewObject(clsInteger, midIntCon, (jint)*(int*)parVal);
            check_exception(status, "Cannot instantiate new int object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Int: %d\n", *(int*)parVal);
            break;
        case long_dt:
            jobjParType = par_type.LONG_T;
            jobjParVal = status->localJniEnv->NewObject(clsLong, midLongCon, (jlong)*(long*)parVal);
            check_exception(status, "Cannot instantiate new long object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Long: %ld\n", *(long*)parVal);
            break;
        case longlong_dt:
        case float_dt:
            jobjParType = par_type.FLOAT_T;
            jobjParVal = status->localJniEnv->NewObject(clsFloat, midFloatCon, (jfloat)*(float*)parVal);
            check_exception(status, "Cannot instantiate new float object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Float: %f\n", *(float*)parVal);
            break;
        case double_dt:
            jobjParType = par_type.DOUBLE_T;
            jobjParVal = status->localJniEnv->NewObject(clsDouble, midDoubleCon, (jdouble)*(double*)parVal);
            check_exception(status, "Cannot instantiate new double object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Double: %f\n", *(double*)parVal);
            break;
        case file_dt:
            jobjParType = par_type.FILE_T;
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for file)");
            debug_printf ("[BINDING-COMMONS] - @process_param - File: %s\n", *(char **)parVal);
            break;
        case directory_dt:
            jobjParType = par_type.DIRECTORY_T;
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for directory)");
            debug_printf ("[BINDING-COMMONS] - @process_param - Directory: %s\n", *(char **)parVal);
            break;
        case external_stream_dt:
            jobjParType = par_type.EXTERNAL_STREAM_T;
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for stream)");
            debug_printf ("[BINDING-COMMONS] - @process_param - External Stream: %s\n", *(char **)parVal);
            break;
        case external_psco_dt:
            jobjParType = par_type.EXTERNAL_PSCO_T;
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for psco)");
            debug_printf ("[BINDING-COMMONS] - @process_param - Persistent: %s\n", *(char **)parVal);
            break;
        case string_dt:
            jobjParType = par_type.STRING_T;
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object");
            debug_printf ("[BINDING-COMMONS] - @process_param - String: %s\n", *(char **)parVal);
            break;
        case string_64_dt:
            jobjParType = par_type.STRING_64_T;
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object");
            debug_printf ("[BINDING-COMMONS] - @process_param - String: %s\n", *(char **)parVal);
            break;
        case binding_object_dt:
            jobjParType = par_type.BINDING_OBJECT_T;
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for binding object)");
            debug_printf ("[BINDING-COMMONS] - @process_param - BindingObject: %s\n", *(char **)parVal);
            break;
        case collection_dt:
            jobjParType = par_type.COLLECTION_T;
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for collection)");
            debug_printf ("[BINDING-COMMONS] - @process_param - Collection: %s\n", *(char **)parVal);
            break;
        case dict_collection_dt:
            jobjParType = par_type.DICT_COLLECTION_T;
            jobjParVal = globalJniEnv -> NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for dictionary collection)");
            debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Dictionary Collection: %s\n", *(char **)parVal);
            break;
        case null_dt:
            jobjParType = par_type.NULL_T;
            jobjParVal = globalJniEnv -> NewStringUTF("NULL");
            check_exception(status, "Cannot instantiate new null object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Null: NULL\n");
            break;
        case void_dt:
        case any_dt:
        default:
            debug_printf ("[BINDING-COMMONS] - @process_param - The type of the parameter %s is not registered\n", *(char **)parName);
            break;
    }

    // Sets the parameter value and type
    status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pv, jobjParVal);
    status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pt, jobjParType);

    // Add param direction
    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM DIRECTION: %d\n", (enum direction) parDirect);
    switch ((enum direction) parDirect) {
        case in_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, par_dir.IN);
            break;
        case out_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, par_dir.OUT);
            break;
        case inout_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, par_dir.INOUT);
            break;
        case concurrent_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, par_dir.CONCURRENT);
            break;
        case commutative_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, par_dir.COMMUTATIVE);
            break;
        case in_delete_dir:
        	status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, par_dir.IN_DELETE);
        	break;
        default:
            break;
    }

    // Add param stream
    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM STD IO STREAM: %d\n", (enum io_stream) parIOStream);
    switch ((enum io_stream) parIOStream) {
        case STD_IN:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, ps, std_stream.STDIN);
            break;
        case STD_OUT:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, ps, std_stream.STDOUT);
            break;
        case STD_ERR:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, ps, std_stream.STDERR);
            break;
        default:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, ps, std_stream.UNSPECIFIED);
            break;
    }

    // Add param prefix
    debug_printf ("[BINDING-COMMONS] - @process_param - PREFIX: %s\n", *(char**)parPrefix);
    jstring jobjParPrefix = status->localJniEnv->NewStringUTF(*(char**)parPrefix);
    status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pp, jobjParPrefix);

    debug_printf ("[BINDING-COMMONS] - @process_param - NAME: %s\n", *(char**)parName);
    jstring jobjParName = status->localJniEnv->NewStringUTF(*(char**)parName);
    status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pn, jobjParName);

    debug_printf ("[BINDING-COMMONS] - @process_param - CONTENT TYPE: %s\n", *(char**)parConType);
    jstring jobConType = status->localJniEnv->NewStringUTF(*(char**)parConType);
    status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pc, jobConType);

    debug_printf ("[BINDING-COMMONS] - @process_param - WEIGHT : %s\n", *(char**)parWeight);
    jstring jobjParWeight = status->localJniEnv->NewStringUTF(*(char**)parWeight);
    status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pw, jobjParWeight);

    debug_printf ("[BINDING-COMMONS] - @process_param - KEEP RENAME : %d\n", parKeepRename);
    bool _KeepRename = false;
    if (parKeepRename != 0) _KeepRename = true;
	jobject jobjParKeepRename = status->localJniEnv->NewObject(clsBoolean, midBoolCon, _KeepRename);
	check_exception(status, "Exception creating a new boolean for keep rename property");
	status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pkr, jobjParKeepRename);


}


// Given a COMPSsException, get its containing message
static void getReceivedException(JNIEnv* env, jthrowable exception, char** buf)
{
    int success = 0;
    jclass exceptionClazz = env->GetObjectClass((jobject)exception);
    jclass classClazz = env->GetObjectClass((jobject)exceptionClazz);
    jmethodID classGetNameMethod = env->GetMethodID(classClazz, "getName", "()Ljava/lang/String;");
    jstring classNameStr = (jstring)env->CallObjectMethod(exceptionClazz, classGetNameMethod);
    if (strcmp(env->GetStringUTFChars(classNameStr ,0), "es.bsc.compss.worker.COMPSsException") != 0) {
        env->ExceptionClear();
    } else {
        const char* classNameChars = env->GetStringUTFChars(classNameStr, NULL);
        if (classNameChars != NULL) {
            jmethodID throwableGetMessageMethod = env->GetMethodID(exceptionClazz, "getMessage", "()Ljava/lang/String;");
            jstring messageStr = (jstring)env->CallObjectMethod(exception, throwableGetMessageMethod);
            if (messageStr != NULL) {
                const char* messageChars = env->GetStringUTFChars( messageStr, NULL);
                if (messageChars != NULL) {
                    *buf = strdup(messageChars);
                    env->ReleaseStringUTFChars(messageStr, messageChars);
                    env->ExceptionClear();
                } else {
                    env->ExceptionClear();
                }
                env->DeleteLocalRef(messageStr);
            }
            env->ReleaseStringUTFChars(classNameStr, classNameChars);
            env->DeleteLocalRef(classNameStr);
        }
        env->DeleteLocalRef(classClazz);
        env->DeleteLocalRef(exceptionClazz);
    }
}

// ******************************
// Workflow functions
// ******************************
long JNI_WF_getId(CompssWorkflow* self) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    
    debug_printf("[BINDING-COMMONS] - @JNI_WF_GetId\n");
    
    // Request thread access to JVM
    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;
    jobject jLong = env->CallObjectMethod(wf->jWorkflow, mid_wf_getID);

    long id = (long) env->CallLongMethod(jLong, midLongVal);
    env->DeleteLocalRef(jLong);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf ("[BINDING-COMMONS] - @JNI_WF_GetId - Obtained id %ld\n", id);
    return id;
}


void JNI_WF_deregister(CompssWorkflow* self) {
    debug_printf("[BINDING-COMMONS] - @JNI_WF_Deregister\n");
    JNIWorkflow* wf = (JNIWorkflow*) self;

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    
    JNIEnv* env = status->localJniEnv;

    env->CallVoidMethod(wf->jWorkflow, mid_wf_deregister);
    check_exception(status, "Workflow.deregister failed");
    env->DeleteGlobalRef(wf->jWorkflow);

    // Revoke thread access to JVM
    access_revoke(status);

}


void JNI_WF_openTaskGroup(CompssWorkflow* self, const char* groupName, bool implicitBarrier) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_openTaskGroup\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jstring jGroup = env->NewStringUTF(groupName);
    env->CallVoidMethod(wf->jWorkflow, mid_wf_openTaskGroup, jGroup, implicitBarrier);
    check_exception(status, "Workflow.openTaskGroup failed");
    env->DeleteLocalRef(jGroup);

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_openTaskGroup - Done\n");
}

void JNI_WF_closeTaskGroup(CompssWorkflow* self, const char* groupName) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_closeTaskGroup\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jstring jGroup = env->NewStringUTF(groupName);
    env->CallVoidMethod(wf->jWorkflow, mid_wf_closeTaskGroup, jGroup);
    check_exception(status, "Workflow.closeTaskGroup failed");
    env->DeleteLocalRef(jGroup);

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_closeTaskGroup - Done\n");
}

void JNI_WF_cancelTaskGroup(CompssWorkflow* self, const char* groupName, char** exceptionMessage) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_cancelTaskGroup\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jstring jGroup = env->NewStringUTF(groupName);
    env->CallVoidMethod(wf->jWorkflow, mid_wf_cancelTaskGroup, jGroup);
    check_and_get_compss_exception(status, exceptionMessage);
    env->DeleteLocalRef(jGroup);

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_cancelTaskGroup - Done\n");
}

void JNI_WF_cancelApplicationTasks(CompssWorkflow* self) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_cancelApplicationTasks\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    env->CallVoidMethod(wf->jWorkflow, mid_wf_cancelApplicationTasks);
    check_exception(status, "Workflow.cancelApplicationTasks failed");

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_cancelApplicationTasks - Done\n");
}

void JNI_WF_noMoreTasks(CompssWorkflow* self) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_noMoreTasks\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    env->CallVoidMethod(wf->jWorkflow, mid_wf_noMoreTasks);
    check_exception(status, "Workflow.noMoreTasks failed");

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_noMoreTasks - Done\n");
}

void JNI_WF_barrier(CompssWorkflow* self) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_barrier\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    env->CallVoidMethod(wf->jWorkflow, mid_wf_barrier);
    check_exception(status, "Workflow.barrier failed");

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_barrier - Done\n");
}

void JNI_WF_barrierWithFlag(CompssWorkflow* self, bool noMoreTasksFlag) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_barrierWithFlag\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    env->CallVoidMethod(wf->jWorkflow, mid_wf_barrier_withFlag, noMoreTasksFlag);
    check_exception(status, "Workflow.barrier(boolean) failed");

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_barrierWithFlag - Done\n");
}

void JNI_WF_barrierGroup(CompssWorkflow* self, const char* groupName, char** exceptionMessage) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_barrierGroup\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jstring jGroup = env->NewStringUTF(groupName);
    env->CallVoidMethod(wf->jWorkflow, mid_wf_barrierGroup, jGroup);
    check_and_get_compss_exception(status, exceptionMessage);
    env->DeleteLocalRef(jGroup);

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_barrierGroup - Done\n");
}

void JNI_WF_snapshot(CompssWorkflow* self) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_snapshot\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    env->CallVoidMethod(wf->jWorkflow, mid_wf_snapshot);
    check_exception(status, "Workflow.snapshot failed");

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_snapshot - Done\n");
}

void JNI_WF_getObject(CompssWorkflow* self, char* fileName, char** buf) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_getObject\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;
    jstring jFileName = env->NewStringUTF(fileName);
    jstring jstr = (jstring)env->CallObjectMethod(wf->jWorkflow, mid_wf_getBindingObject, jFileName);
    check_exception(status, "Workflow.getObject failed");

    // Parse output
    jboolean isCopy;
    const char* cstr = env->GetStringUTFChars(jstr, &isCopy);
    *buf = strdup(cstr);
    env->ReleaseStringUTFChars(jstr, cstr);
    env->DeleteLocalRef(jstr);
    env->DeleteLocalRef(jFileName);

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_getObject - Done\n");
}

void JNI_WF_deleteObject(CompssWorkflow* self, char* fileName, int** buf) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_deleteObject\n");

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jstring jFileName = env->NewStringUTF(fileName);
    jboolean res = env->CallBooleanMethod(wf->jWorkflow, mid_wf_deleteBindingObject, jFileName);
    check_exception(status, "Workflow.deleteObject failed");
    *buf = (int*) &res;
    env->DeleteLocalRef(jFileName);

    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_deleteObject - Done\n");
}



CompssWorkflow* JNI_RegisterWorkflow() {
    debug_printf("[BINDING-COMMONS] - @JNI_RegisterWorkflow\n");
    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;
    // Register Worfklow
    jobject jWorkflowObj = env->CallObjectMethod(globalRuntime, midRegWf, NULL, NULL);

    if (clsWorkflow == NULL) {
        clsWorkflow = env->GetObjectClass(jWorkflowObj);

        mid_wf_getID = env->GetMethodID(clsWorkflow, "getId", "()Ljava/lang/Long;");
        check_exception(status, "Cannot find the Workflow.getId method");
        mid_wf_openTaskGroup = env->GetMethodID(clsWorkflow, "openTaskGroup", "(Ljava/lang/String;Z)V");
        check_exception(status, "Cannot find the Workflow.openTaskGroup method");
        mid_wf_closeTaskGroup = env->GetMethodID(clsWorkflow, "closeTaskGroup", "(Ljava/lang/String;)V");
        check_exception(status, "Cannot find the Workflow.closeTaskGroup method");
        mid_wf_cancelTaskGroup = env->GetMethodID(clsWorkflow, "cancelTaskGroup", "(Ljava/lang/String;)V");
        check_exception(status, "Cannot find the Workflow.cancelTaskGroup method");
        mid_wf_cancelApplicationTasks = env->GetMethodID(clsWorkflow, "cancelApplicationTasks", "()V");
        check_exception(status, "Cannot find the Workflow.cancelApplicationTasks method");
        mid_wf_noMoreTasks = env->GetMethodID(clsWorkflow, "noMoreTasks", "()V");
        check_exception(status, "Cannot find the Workflow.noMoreTasks method");
        mid_wf_barrier = env->GetMethodID(clsWorkflow, "barrier", "()V");
        check_exception(status, "Cannot find the Workflow.barrier method");
        mid_wf_barrier_withFlag = env->GetMethodID(clsWorkflow, "barrier", "(Z)V");
        check_exception(status, "Cannot find the Workflow.barrier method");
        mid_wf_barrierGroup = env->GetMethodID(clsWorkflow, "barrierGroup", "(Ljava/lang/String;)V");
        check_exception(status, "Cannot find the Workflow.barrierGroup method");
        mid_wf_snapshot = env->GetMethodID(clsWorkflow, "snapshot", "()V");
        check_exception(status, "Cannot find the Workflow.snapshot method");
        mid_wf_deregister = env->GetMethodID(clsWorkflow, "deregister", "()V");
        check_exception(status, "Cannot find the Workflow.deregister  method");

        // Data operations
        mid_wf_getBindingObject = env->GetMethodID(clsWorkflow, "getBindingObject", "(Ljava/lang/String;)Ljava/lang/String;");
        check_exception(status, "Cannot find getBindingObject");
        mid_wf_deleteBindingObject = env->GetMethodID(clsWorkflow, "deleteBindingObject", "(Ljava/lang/String;)Z");
        check_exception(status, "Cannot find deleteBindingObject");
    }

    // Wrap Java Workflow object into a C struct implementing the interface
    JNIWorkflow* wf = (JNIWorkflow*) malloc(sizeof(JNIWorkflow));
    wf->jWorkflow = env->NewGlobalRef(jWorkflowObj);

    wf->base.getId = JNI_WF_getId;
    wf->base.deregister = JNI_WF_deregister;
    wf->base.openTaskGroup = JNI_WF_openTaskGroup;
    wf->base.closeTaskGroup = JNI_WF_closeTaskGroup;
    wf->base.cancelTaskGroup = JNI_WF_cancelTaskGroup;
    wf->base.cancelApplicationTasks = JNI_WF_cancelApplicationTasks;
    wf->base.noMoreTasks = JNI_WF_noMoreTasks;
    wf->base.barrier = JNI_WF_barrier;
    wf->base.barrierWithFlag = JNI_WF_barrierWithFlag;
    wf->base.barrierGroup = JNI_WF_barrierGroup;
    wf->base.snapshot = JNI_WF_snapshot;
    wf->base.get_object = JNI_WF_getObject;
    wf->base.delete_object = JNI_WF_deleteObject;


    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf ("[BINDING-COMMONS] - @JNI_RegisterWorkflow - Workflow registered\n");
    return (CompssWorkflow*)wf;
}

// ******************************
// API functions
// ******************************


void JNI_On() {
    debug_printf ("[BINDING-COMMONS] - @JNI_On\n");

    // Initialise COMPSs env vars for debugging (from commons.h)
    debug_printf ("[BINDING-COMMONS] - @JNI_On - Initialising environment\n");
    pthread_mutex_init(&globalJniAccessMutex, NULL);
    init_env_vars();

    // Create the JVM instance
    debug_printf ("[BINDING-COMMONS] - @JNI_On - Creating the JVM\n");
    globalJniEnv = create_vm(&globalJvm);
    if (globalJniEnv == NULL) {
        print_error ("[BINDING-COMMONS] - @JNI_On - Error creating the JVM\n");
        exit(1);
    }

    // Request thread access to JVM
    // debug_printf ("[BINDING-COMMONS] - @JNI_On - Request thread access to JVM\n");
    ThreadStatus* status = access_request();

    // Obtain Runtime classes
    debug_printf ("[BINDING-COMMONS] - @JNI_On - Obtaining Runtime classes\n");
    jclass clsITimpl = NULL;
    jmethodID midITImplConst = NULL;
    jmethodID midStartIT = NULL;

    jclass clsLocal = status->localJniEnv->FindClass("es/bsc/compss/api/impl/COMPSsRuntimeImpl");
    check_exception(status, "Cannot find the COMPSsRuntimeImpl class");
    clsITimpl = (jclass) status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot instantiate the COMPSsRuntimeImpl class");

    if (clsITimpl != NULL) {
        // Get constructor ID for COMPSsRuntimeImpl
        midITImplConst = status->localJniEnv->GetMethodID(clsITimpl, "<init>", "()V");
        check_exception(status, "Cannot find the COMPSsRuntimeImpl init method");

        // Get startIT method ID
        midStartIT = status->localJniEnv->GetMethodID(clsITimpl, "startIT", "()V");
        check_exception(status, "Cannot find the startIT method");
    } else {
        print_error("[BINDING-COMMONS] - @JNI_On - Unable to find the runtime class\n");
        exit(1);
    }

    if (midITImplConst != NULL) {
        // Creating the Object of IT.
        debug_printf ("[BINDING-COMMONS] - @JNI_On - Creating runtime object\n");
        jobject objLocal = status->localJniEnv->NewObject(clsITimpl, midITImplConst);
        check_exception(status, "Cannot instantiate the COMPSsRuntimeImpl object");
        globalRuntime = (jobject) status->localJniEnv->NewGlobalRef(objLocal);
        check_exception(status, "Cannot create global COMPSsRuntimeImpl object");
    } else {
        print_error("[BINDING-COMMONS] - @JNI_On - Unable to find the runtime constructor\n");
        exit(1);
    }

    if (globalRuntime != NULL && midStartIT != NULL) {
        debug_printf ("[BINDING-COMMONS] - @JNI_On - Calling runtime start\n");
        status->localJniEnv->CallVoidMethod(globalRuntime, midStartIT); //Calling the method and passing IT Object as parameter
        check_exception(status, "Exception calling start runtime");
    } else {
        print_error("[BINDING-COMMONS] - @JNI_On - Unable to find the start method\n");
        exit(1);
    }

    // Init basic JNI types
    init_basic_jni_types(status);

    // Init master types
    init_master_jni_types(status, clsITimpl);

    // Revoke thread access to JVM
    // debug_printf ("[BINDING-COMMONS] - @JNI_On - Revoke thread access to JVM\n");
    access_revoke(status);

    CompssWorkflow* wf = JNI_RegisterWorkflow();
    JNI_wf = wf;
    long wf_id = wf->getId(wf);
    JNI_wf_appId = wf_id;
    debug_printf("[BINDING-COMMONS] - @JNI_On REgistered Workflow with id %ld\n", wf_id);
}


void JNI_Off(int code) {
    debug_printf("[BINDING-COMMONS] - @JNI_Off\n");

    JNI_wf->noMoreTasks(JNI_wf);
    
    // Request thread access to JVM
    // debug_printf ("[BINDING-COMMONS] - @JNI_Off - Request thread access to JVM\n");
    ThreadStatus* status = access_request();

    // Call stopIT
    debug_printf("[BINDING-COMMONS] - @Off - Stopping runtime\n");
    status->localJniEnv->CallVoidMethod(globalRuntime, midStopIT, "TRUE");
    check_exception(status, "Exception received when calling stopIT.");

    // Revoke thread access to JVM
    debug_printf("[BINDING-COMMONS] - @Off - Revoke thread access to JVM\n");
    access_revoke(status);

    // Remove JVM
    debug_printf("[BINDING-COMMONS] - @Off - Removing JVM\n");
    destroy_vm(globalJvm);  // Release jvm resources -- Does not work properly --> JNI bug: not releasing properly the resources, so it is not possible to recreate de JVM.
    // delete jvm;    // free(): invalid pointer: 0x00007fbc11ba8020 ***
    globalJvm = NULL;

    // Delete environment
    debug_printf("[BINDING-COMMONS] - @Off - Removing environment\n");
    pthread_mutex_destroy(&globalJniAccessMutex);

    // End
    debug_printf("[BINDING-COMMONS] - @Off - End\n");
}


void JNI_read_command(char** command){
    // Do nothing
}

void JNI_Cancel_Application_Tasks(long appId) {
    JNI_wf->cancelApplicationTasks(JNI_wf);
}


void JNI_Get_AppDir(char** buf) {
    debug_printf ("[BINDING-COMMONS] - @JNI_Get_AppDir - Getting application directory.\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    jstring jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime, midAppDir);
    check_exception(status, "Exception received when calling getAppDir");

    // Parse return
    jboolean isCopy;
    const char* cstr = status->localJniEnv->GetStringUTFChars(jstr, &isCopy);
    *buf = strdup(cstr);
    status->localJniEnv->ReleaseStringUTFChars(jstr, cstr);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Get_AppDir - directory name: %s\n", *buf);
}


void JNI_Get_MasterWorkingDir(char** buf) {
    debug_printf ("[BINDING-COMMONS] - @JNI_Get_MasterWorkingDir - Getting Master Working directory.\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    jstring jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime, midTempDir);
    check_exception(status, "Exception received when calling getMasterWorkingDir");

    // Parse return
    jboolean isCopy;
    const char* cstr = status->localJniEnv->GetStringUTFChars(jstr, &isCopy);
    *buf = strdup(cstr);
    status->localJniEnv->ReleaseStringUTFChars(jstr, cstr);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Get_MasterWorkingDir - directory name: %s\n", *buf);
}


void JNI_ExecuteTask(long appId, char* className, char* onFailure, int timeout, char* methodName, int priority, int numNodes, int reduce, int reduceChunkSize,
		int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {

    debug_printf ("[BINDING-COMMONS] - @JNI_ExecuteTask - Processing task execution in bindings-common.\n");

    // Values to be passed to the JVM
    jobjectArray jobjOBJArr; /* array of Objects to be passed to executeTask */

    bool _priority = false;
    if (priority != 0) _priority = true;

    bool _reduce = false;
    if (reduce != 0) _reduce = true;

    bool _replicated = false;
    if (replicated != 0) _replicated = true;

    bool _distributed = false;
    if (distributed != 0) _distributed = true;

    bool _hasTarget = false;
    if (hasTarget != 0) _hasTarget = true;

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Convert numReturns from int to integer
    jobject numReturnsInteger = status->localJniEnv->NewObject(clsInteger, midIntCon, numReturns);
    check_exception(status, "Exception converting numReturns to integer");

    // Create array of parameters
    jobjOBJArr = (jobjectArray)status->localJniEnv->NewObjectArray(numParams * NUM_FIELDS, clsObject, status->localJniEnv->NewObject(clsObject, midObjCon));
    for (int i = 0; i < numParams; i++) {
        debug_printf("[BINDING-COMMONS] - @JNI_ExecuteTask - Processing parameter %d\n", i);
        process_param(status, params, i, jobjOBJArr);
    }

    // Call to JNI execute task method
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midExecute,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                              status->localJniEnv->NewStringUTF(className),
                              status->localJniEnv->NewStringUTF(onFailure),
                              timeout,
                              status->localJniEnv->NewStringUTF(methodName),
                              _priority,
							  numNodes,
							  _reduce,
							  reduceChunkSize,
							  _replicated,
							  _distributed,
                              _hasTarget,
                              numReturnsInteger,
                              numParams,
                              jobjOBJArr);
    check_exception(status, "Exception received when calling executeTask");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf ("[BINDING-COMMONS] - @JNI_ExecuteTask - Task processed.\n");
}


void JNI_ExecuteTaskNew(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize,
                        int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {

    debug_printf ("[BINDING-COMMONS] - @JNI_ExecuteTaskNew - Processing task execution in bindings-common. \n");

    // Values to be passed to the JVM
    jobjectArray jobjOBJArr; /* array of Objects to be passed to executeTask */

    bool _priority = false;
    if (priority != 0) _priority = true;

    bool _replicated = false;
    if (replicated != 0) _replicated = true;

    bool _reduce = false;
    if (reduce != 0) _reduce = true;

    bool _distributed = false;
    if (distributed != 0) _distributed = true;

    bool _hasTarget = false;
    if (hasTarget != 0) _hasTarget = true;

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Convert numReturns from int to integer
    jobject numReturnsInteger = status->localJniEnv->NewObject(clsInteger, midIntCon, numReturns);
    check_exception(status, "Exception converting numReturns to integer");

    // Create array of parameters
    jobjOBJArr = (jobjectArray)status->localJniEnv->NewObjectArray(numParams * NUM_FIELDS, clsObject, status->localJniEnv->NewObject(clsObject, midObjCon));
    for (int i = 0; i < numParams; i++) {
        debug_printf("[BINDING-COMMONS] - @JNI_ExecuteTaskNew - Processing parameter %d\n", i);
        process_param(status, params, i, jobjOBJArr);
    }

    // Call to JNI execute task method
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midExecuteNew,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                              status->localJniEnv->NewStringUTF(signature),
                              status->localJniEnv->NewStringUTF(onFailure),
                              timeout,
                              _priority,
                              numNodes,
                              _reduce,
                              reduceChunkSize,
                              _replicated,
                              _distributed,
                              _hasTarget,
                              numReturnsInteger,
                              numParams,
                              jobjOBJArr);
    check_exception(status, "Exception received when calling executeTaskNew");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf ("[BINDING-COMMONS] - @JNI_ExecuteTaskNew - Task processed.\n");
}


void JNI_ExecuteHttpTask(long appId, char* signature, char* onFailure, int timeout, int priority, int numNodes, int reduce,
                         int reduceChunkSize, int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {

    debug_printf ("[BINDING-COMMONS] - @JNI_ExecuteHttpTask - HTTP task execution in bindings-common. \n");

    // Values to be passed to the JVM
    jobjectArray jobjOBJArr; /* array of Objects to be passed to executeTask */

    bool _priority = false;
    if (priority != 0) _priority = true;

    bool _replicated = false;
    if (replicated != 0) _replicated = true;

    bool _reduce = false;
    if (reduce != 0) _reduce = true;

    bool _distributed = false;
    if (distributed != 0) _distributed = true;

    bool _hasTarget = false;
    if (hasTarget != 0) _hasTarget = true;

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    jobject jobjOnFailure = NULL;

    if(onFailure == NULL){
        debug_printf ("[BINDING-COMMONS] - @JNI_ExecuteHttpTask - HTTP task execution in bindings-common on failure is null. \n");
        jobjOnFailure = status->localJniEnv->CallStaticObjectMethod(clsOnFailure, midOnFailureCon, status->localJniEnv->NewStringUTF("RETRY"));
    }
    else{
         jobjOnFailure = status->localJniEnv->CallStaticObjectMethod(clsOnFailure, midOnFailureCon, status->localJniEnv->NewStringUTF(onFailure));
         check_exception(status, "Exception Creating OnFailure object..");
    }

    // Convert numReturns from int to integer
    jobject numReturnsInteger = status->localJniEnv->NewObject(clsInteger, midIntCon, numReturns);
    check_exception(status, "Exception converting numReturns to integer");

    // Create array of parameters
    jobjOBJArr = (jobjectArray)status->localJniEnv->NewObjectArray(numParams * NUM_FIELDS, clsObject, status->localJniEnv->NewObject(clsObject, midObjCon));
    for (int i = 0; i < numParams; i++) {
        debug_printf("[BINDING-COMMONS] - @JNI_ExecuteHttpTask- Processing parameter %d\n", i);
        process_param(status, params, i, jobjOBJArr);
    }

    // Call to JNI execute task method
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midExecuteHttp,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                              status->localJniEnv->NewStringUTF(signature), // declaring method
                              _priority,
                              numNodes,
                              _reduce,
                              reduceChunkSize,
                              _replicated,
                              _distributed,
                              _hasTarget,
                              numParams,
                              jobjOnFailure,
                              timeout,
                              jobjOBJArr);

    check_exception(status, "Exception received when calling executeHttpTask");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf ("[BINDING-COMMONS] - @JNI_ExecuteHttpTask - HTTP Task processed.\n");
}


void JNI_RegisterCE(char* ceSignature, char* implSignature, char* implConstraints, char* implType, char* implLocal, char* implIO, char** prolog, char** epilog, char** container, int numParams, char** implTypeArgs) {
    //debug_printf ("[BINDING-COMMONS] - @JNI_RegisterCE - ceSignature:     %s\n", ceSignature);
    //debug_printf ("[BINDING-COMMONS] - @JNI_RegisterCE - implSignature:   %s\n", implSignature);
    //debug_printf ("[BINDING-COMMONS] - @JNI_RegisterCE - implConstraints: %s\n", implConstraints);
    //debug_printf ("[BINDING-COMMONS] - @JNI_RegisterCE - implType:        %s\n", implType);
    //debug_printf ("[BINDING-COMMONS] - @JNI_RegisterCE - implLocal:        %s\n", implLocal);
    //debug_printf ("[BINDING-COMMONS] - @JNI_RegisterCE - implIO:        %s\n", implIO);
    //debug_printf ("[BINDING-COMMONS] - @JNI_RegisterCE - numParams:      %d\n", numParams);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Array of Objects to pass to the register
    jobjectArray prologArr;
    jobjectArray epilogArr;
    jobjectArray containerArr;
    prologArr = (jobjectArray)status->localJniEnv->NewObjectArray(3, clsString, status->localJniEnv->NewStringUTF(""));
    epilogArr = (jobjectArray)status->localJniEnv->NewObjectArray(3, clsString, status->localJniEnv->NewStringUTF(""));
    containerArr = (jobjectArray)status->localJniEnv->NewObjectArray(3, clsString, status->localJniEnv->NewStringUTF(""));
    for (int i = 0; i < 3; i++) {
        //debug_printf("[BINDING-COMMONS] - @JNI_RegisterCE -   Processing pos %d\n", i);
        jstring tmpro = status->localJniEnv->NewStringUTF(prolog[i]);
        jstring tmpepi = status->localJniEnv->NewStringUTF(epilog[i]);
        jstring tmpcont = status->localJniEnv->NewStringUTF(container[i]);
        status->localJniEnv->SetObjectArrayElement(prologArr, i, tmpro);
        status->localJniEnv->SetObjectArrayElement(epilogArr, i, tmpepi);
        status->localJniEnv->SetObjectArrayElement(containerArr, i, tmpcont);
    }

    // Array of Objects to pass to the register
    jobjectArray implArgs;
    implArgs = (jobjectArray)status->localJniEnv->NewObjectArray(numParams, clsString, status->localJniEnv->NewStringUTF(""));
    for (int i = 0; i < numParams; i++) {
        //debug_printf("[BINDING-COMMONS] - @JNI_RegisterCE -   Processing pos %d\n", i);
        jstring tmp = status->localJniEnv->NewStringUTF(implTypeArgs[i]);
        status->localJniEnv->SetObjectArrayElement(implArgs, i, tmp);
    }

    //debug_printf("[BINDING-COMMONS] - @JNI_RegisterCE -   Calling Runtime Function Register Core Element \n");
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midRegisterCE,
                              status->localJniEnv->NewStringUTF(ceSignature),
                              status->localJniEnv->NewStringUTF(implSignature),
                              status->localJniEnv->NewStringUTF(implConstraints),
                              status->localJniEnv->NewStringUTF(implType),
                              status->localJniEnv->NewStringUTF(implLocal),
                              status->localJniEnv->NewStringUTF(implIO),
                              prologArr,
                              epilogArr,
                              containerArr,
                              implArgs);
    check_exception(status, "Exception received when calling registerCE");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_RegisterCE - Task registered: %s\n", ceSignature);
}

int JNI_Accessed_File(long appId, char* fileName){
    debug_printf("[BINDING-COMMONS] - @JNI_Accessed_File - Calling runtime isFileAccessed method  for %s  ...\n", fileName);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Format filename
	jstring filename_str = status->localJniEnv->NewStringUTF(fileName);
	check_exception(status, "Error getting String UTF");

    // Perform operation
	jboolean is_accessed = (jboolean)status->localJniEnv->CallBooleanMethod(globalRuntime,
                                                                midIsFileAccessed,
                                                                status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                                                filename_str);
    check_exception(status, "Error calling runtime isFileAccessed");
    status->localJniEnv->DeleteLocalRef(filename_str);

    // Parse result
    int ret = 0;
    if ((bool) is_accessed) {
    	ret = 1;
    }

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Accessed_File - Access to file %s marked as %d\n", fileName, ret);
    return ret;
}

void JNI_Open_File(long appId, char* fileName, int mode, char** buf) {
    debug_printf("[BINDING-COMMONS] - @JNI_Open_File - Calling runtime OpenFile method  for %s and mode %d ...\n", fileName, mode);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Parse fileName
    jstring filename_str = status->localJniEnv->NewStringUTF(fileName);
    check_exception(status, "Error getting String UTF");

    // Call operation
    jstring jstr = NULL;
    switch ((enum direction) mode) {
        case in_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                                        filename_str,
                                                        par_dir.IN);
            break;
        case out_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                                        filename_str,
                                                        par_dir.OUT);
            break;
        case inout_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                                        filename_str,
                                                        par_dir.INOUT);
            break;
        case concurrent_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                                        filename_str,
                                                        par_dir.CONCURRENT);
            break;
        case commutative_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                                        filename_str,
                                                        par_dir.COMMUTATIVE);
            break;
        default:
            break;
    }
    check_exception(status, "Exception calling runtime openFile");
    status->localJniEnv->DeleteLocalRef(filename_str);

    // Parse output
    jboolean isCopy;
    const char* cstr = status->localJniEnv->GetStringUTFChars(jstr, &isCopy);
    check_exception(status, "Exception getting String UTF");

    *buf = strdup(cstr);
    status->localJniEnv->ReleaseStringUTFChars(jstr, cstr);
    status->localJniEnv->DeleteLocalRef(jstr);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Open_File - COMPSs filename: %s\n", *buf);
}


void JNI_Close_File(long appId, char* fileName, int mode) {
    debug_printf("[BINDING-COMMONS] - @JNI_Close_File - Calling runtime closeFile method...\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    switch ((enum direction) mode) {
        case in_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        par_dir.IN);
            break;
        case out_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        par_dir.OUT);
            break;
        case inout_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        par_dir.INOUT);
            break;
        case concurrent_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        par_dir.CONCURRENT);
            break;
        case commutative_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        par_dir.COMMUTATIVE);
            break;
        default:
            break;
    }
    check_exception(status, "Exception calling runtime closeFile");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Close_File - COMPSs filename: %s\n", fileName);
}


void JNI_Delete_File(long appId, char* fileName, int wait, int applicationDelete) {
    debug_printf("[BINDING-COMMONS] - @JNI_Delete_File - Calling runtime deleteFile method...\n");

    // Local variables for JVM call
    bool _wait = false;
    if (wait != 0) _wait = true;

    bool _applicationDelete = false;
    if (applicationDelete != 0) _applicationDelete = true;

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    jboolean res = status->localJniEnv->CallBooleanMethod(globalRuntime,
                                            midDeleteFile,
                                            status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                            status->localJniEnv->NewStringUTF(fileName),
                                            _wait,
                                            _applicationDelete);
    check_exception(status, "Exception received when calling deleteFile");
    //*buf = (int*)&res;

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Delete_File - COMPSs filename: %s\n", fileName);
    debug_printf("[BINDING-COMMONS] - @JNI_Delete_File - File erased with status: %i\n", (bool) res);
}


void JNI_Get_File(long appId, char* fileName) {
    debug_printf("[BINDING-COMMONS] - @JNI_Get_File - Calling runtime getFile method...\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                                midGetFile,
                                status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                status->localJniEnv->NewStringUTF(fileName));
    check_exception(status, "Exception received when calling getFile");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Get_File - COMPSs filename: %s\n", fileName);
}

void JNI_Get_Directory(long appId, char* dirName) {
    debug_printf("[BINDING-COMMONS] - @JNI_Get_Directory - Calling runtime getDirectory method...\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                                midGetDirectory,
                                status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                                status->localJniEnv->NewStringUTF(dirName));
    check_exception(status, "Exception received when calling getDirectory");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Get_Directory - COMPSs directory: %s\n", dirName);
}

void JNI_Get_Object(long appId, char* fileName, char** buf) {
    debug_printf("[BINDING-COMMONS] - @JNI_Get_Object - Calling runtime getObject method...\n");
    JNI_wf->get_object(JNI_wf, fileName, buf);
    debug_printf("[BINDING-COMMONS] - @JNI_Get_Object - COMPSs data id: %s\n", *buf);
}


void JNI_Delete_Object(long appId, char* fileName, int** buf) {
    debug_printf("[BINDING-COMMONS] - @JNI_Delete_Object - Calling runtime deleteObject method...\n");
    JNI_wf->delete_object(JNI_wf, fileName, buf);
    debug_printf("[BINDING-COMMONS] - @JNI_Delete_Binding_Object - COMPSs obj: %s\n", fileName);
}


void JNI_Barrier(long appId) {
    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - Waiting tasks for APP id: %lu\n", appId);
	JNI_wf->barrier(JNI_wf);
    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - APP id: %lu\n", appId);
}


void JNI_BarrierNew(long appId, int noMoreTasks) {
    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - Waiting tasks for APP id: %lu\n", appId);

    // Local variables for JVM call
    bool _noMoreTasks = false;
    if (noMoreTasks != 0) _noMoreTasks = true;
    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - noMoreTasks: %s\n", _noMoreTasks ? "true":"false");
    JNI_wf->barrierWithFlag(JNI_wf, noMoreTasks);
    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - APP id: %lu\n", appId);
}


void JNI_BarrierGroup(long appId, char* groupName, char** exceptionMessage) {
    debug_printf("[BINDING-COMMONS] - @JNI_BarrierGroup - COMPSs group name: %s\n", groupName);
    JNI_wf->barrierGroup(JNI_wf, groupName, exceptionMessage);
    debug_printf("[BINDING-COMMONS] - @JNI_BarrierGroup - Barrier ended for COMPSs group name: %s\n", groupName);
}


void JNI_OpenTaskGroup(char* groupName, int implicitBarrier, long appId){
    debug_printf("[BINDING-COMMONS] - @JNI_OpenTaskGroup - Opening task group...\n");
    JNI_wf->openTaskGroup(JNI_wf, groupName, implicitBarrier);
    debug_printf("[BINDING-COMMONS] - @JNI_OpenTaskGroup - COMPSs group name: %s\n", groupName);
}


void JNI_CloseTaskGroup(char* groupName, long appId){
    debug_printf("[BINDING-COMMONS] - @JNI_CloseTaskGroup - COMPSs group name: %s\n", groupName);
    JNI_wf->closeTaskGroup(JNI_wf, groupName);
    debug_printf("[BINDING-COMMONS] - @JNI_CloseTaskGroup - Task group %s closed.\n", groupName);
}

void JNI_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage){
    debug_printf("[BINDING-COMMONS] - @JNI_CancelTaskGroup - COMPSs group name: %s\n", groupName);
    JNI_wf->cancelTaskGroup(JNI_wf, groupName, exceptionMessage);
    debug_printf("[BINDING-COMMONS] - @JNI_CancelTaskGroup - Task group %s canceled.\n", groupName);
}

void JNI_Snapshot(long appId) {
	debug_printf("[BINDING-COMMONS] - @JNI_Snapshot - Snapshot for APP id: %lu\n", appId);
    JNI_wf->snapshot(JNI_wf);
    debug_printf("[BINDING-COMMONS] - @JNI_Snapshot - APP id: %lu\n", appId);
}

void JNI_EmitEvent(int type, long id) {
    debug_printf("[BINDING-COMMONS] - @JNI_EmitEvent - Emit Event\n");

    // Check validity
    if (type < 0  or id < 0) {
        debug_printf ("[BINDING-COMMONS] - @JNI_EmitEvent - Error: event type and ID must be positive integers, but found: type: %u, ID: %lu\n", type, id);

        JNI_Off(1);
        exit(1);
    }

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    debug_printf ("[BINDING-COMMONS] - @JNI_EmitEvent - Type: %u, ID: %lu\n", type, id);
    status->localJniEnv->CallVoidMethod(globalRuntime, midEmitEvent, type, id);
    check_exception(status, "Exception received when calling emitEvent");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_EmitEvent - Event emitted\n");
}


int JNI_GetNumberOfResources(long appId) {
    debug_printf("[BINDING-COMMONS] - @JNI_GetNumberOfResources - Requesting number of resources\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    jint resources = status->localJniEnv->CallIntMethod(globalRuntime, midGetNumberOfResources);
    check_exception(status, "Exception received when calling getNumberOfResources");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_GetNumberOfResources - Number of active resources %u\n", (int) resources);
    return (int) resources;
}

void JNI_RequestResources(long appId, int numResources, char* groupName) {
    debug_printf("[BINDING-COMMONS] - @JNI_RequestResources - Requesting resources for APP id: %lu\n", appId);
    debug_printf("[BINDING-COMMONS] - @JNI_RequestResources - numResources: %u\n", numResources);
    debug_printf("[BINDING-COMMONS] - @JNI_RequestResources - groupName: %s\n", groupName);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midRequestResources,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                              numResources,
                              status->localJniEnv->NewStringUTF(groupName));
    check_exception(status, "Exception received when calling requestResources");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_RequestResources - Resources creation requested");
}

void JNI_FreeResources(long appId, int numResources, char* groupName) {
    debug_printf("[BINDING-COMMONS] - @JNI_FreeResources - Freeing resources for APP id: %lu\n", appId);
    debug_printf("[BINDING-COMMONS] - @JNI_FreeResources - numResources: %u\n", numResources);
    debug_printf("[BINDING-COMMONS] - @JNI_FreeResources - groupName: %s\n", groupName);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation

    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midFreeResources,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
                              numResources,
                              status->localJniEnv->NewStringUTF(groupName));
    check_exception(status, "Exception received when calling freeResources");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_FreeResources - Resources destruction requested");
}

void JNI_set_wall_clock(long appId, long wcl, int stopRT){
	debug_printf("[BINDING-COMMONS] - @JNI_set_wall_clock - Setting wall clock limit for APP id:%lu of %lu seconds\n", appId, wcl);
	// Request thread access to JVM
	ThreadStatus* status = access_request();
	bool _stop = false;
	if (stopRT != 0) _stop = true;
	// Perform operation

	status->localJniEnv->CallVoidMethod(globalRuntime, midSetWallClockLimit,
			status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) JNI_wf_appId),
			wcl, _stop);
	check_exception(status, "Exception received when calling setWallClockLimit");

	// Revoke thread access to JVM
	access_revoke(status);
}

CompssInterface setup_JNI_runtime(){
    CompssInterface iface{};
    iface.On = JNI_On;
    iface.Off = JNI_Off;
    iface.read_command = JNI_read_command;

    iface.Get_AppDir = JNI_Get_AppDir;
    iface.Get_MasterWorkingDir = JNI_Get_MasterWorkingDir;
    iface.Set_wall_clock = JNI_set_wall_clock;
    
    iface.EmitEvent = JNI_EmitEvent;
    iface.GetNumberOfResources = JNI_GetNumberOfResources;
    iface.RequestResources = JNI_RequestResources;
    iface.FreeResources = JNI_FreeResources;

    
    iface.registerWorkflow = JNI_RegisterWorkflow;
    iface.RegisterCE = JNI_RegisterCE;

    iface.ExecuteTask = JNI_ExecuteTask;
    iface.ExecuteTaskNew = JNI_ExecuteTaskNew;
    iface.ExecuteHttpTask = JNI_ExecuteHttpTask;
    iface.Cancel_Application_Tasks = JNI_Cancel_Application_Tasks;
    iface.Accessed_File = JNI_Accessed_File;
    iface.Open_File = JNI_Open_File;
    iface.Close_File = JNI_Close_File;
    iface.Delete_File = JNI_Delete_File;
    iface.Get_File = JNI_Get_File;
    iface.Get_Directory = JNI_Get_Directory;
    iface.Barrier = JNI_Barrier;
    iface.BarrierNew = JNI_BarrierNew;
    iface.BarrierGroup = JNI_BarrierGroup;
    iface.OpenTaskGroup = JNI_OpenTaskGroup;
    iface.CloseTaskGroup = JNI_CloseTaskGroup;
    iface.CancelTaskGroup = JNI_CancelTaskGroup;
    iface.Snapshot = JNI_Snapshot;

    iface.Get_Object = JNI_Get_Object;
    iface.Delete_Object = JNI_Delete_Object;
    return iface;
}
