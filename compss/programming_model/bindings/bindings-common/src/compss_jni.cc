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

#include "common.h"
#include "common_jni.h"
#include "compss_interface.h"
#include "compss_jni.h"
#include "param_metadata.h"
#include "BindingDataManager.h"

using namespace std;

typedef struct JNIWorkflow {
    CompssWorkflow base;
    jobject jWorkflow;
} JNIWorkflow;

jobject globalRuntime;

CompssWorkflow* JNI_wf;

jmethodID midStopIT;                    /* ID of the stopIT method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midAppDir;                    /* ID of the getApplicationDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midTempDir;                   /* ID of the getTempDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midRegWf;                     /* ID of the registerWorkflow method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midRegisterCE;                /* ID of the RegisterCE method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jclass clsWorkflow;                     /* Class implementing the Workflow interface at runtime */
jmethodID mid_wf_getID;  
jmethodID mid_wf_deregister;
jmethodID mid_wf_openTaskGroup;
jmethodID mid_wf_closeTaskGroup;
jmethodID mid_wf_execute;
jmethodID mid_wf_cancelTaskGroup;
jmethodID mid_wf_cancelApplicationTasks;
jmethodID mid_wf_noMoreTasks;
jmethodID mid_wf_barrier;
jmethodID mid_wf_barrier_withFlag;
jmethodID mid_wf_barrierGroup;
jmethodID mid_wf_snapshot;
jmethodID mid_wf_isFileAccessed;
jmethodID mid_wf_openFile;
jmethodID mid_wf_getFile;
jmethodID mid_wf_closeFile;
jmethodID mid_wf_deleteFile;
jmethodID mid_wf_getDirectory;
jmethodID mid_wf_getBindingObject;
jmethodID mid_wf_deleteBindingObject;

jclass clsOnFailure;
jmethodID midOnFailureCon;

jobject BYTE_CACHE[256];

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

void init_byte_object_cache(ThreadStatus* status) {
    JNIEnv* env = status->localJniEnv;

    jclass clsByte = NULL;
    clsByte = env->FindClass("java/lang/Byte");
    check_exception(status, "Cannot load Byte class");

    jmethodID midByteCon = NULL;
    midByteCon = env->GetStaticMethodID(clsByte, "valueOf", "(B)Ljava/lang/Byte;");
    check_exception(status, "Cannot get Byte constructor");


    for (int i = 0; i < 256; i++) {
        jbyte v = (jbyte)i;
        jobject obj = env->CallStaticObjectMethod(clsByte, midByteCon, v);
        check_exception(status, "Cannot create Byte object");
        BYTE_CACHE[i] = env->NewGlobalRef(obj);
        env->DeleteLocalRef(obj);
    }
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

/**
 * Initialises the COMPSs related types.
 */
void init_master_jni_types(ThreadStatus* status, jclass clsITimpl) {
    debug_printf ("[BINDING-COMMONS] - @Init JNI Master\n");

    // JNI API method calls
    debug_printf ("[BINDING-COMMONS] - @Init JNI Methods\n");

    midRegWf = status->localJniEnv->GetMethodID(clsITimpl, "registerWorkflow", "(Ljava/lang/String;Les/bsc/compss/api/WorkflowListener;)Les/bsc/compss/api/Workflow;");
    check_exception(status, "Cannot find registerWorkflow method");

    // getApplicationDirectory method
    midAppDir = status->localJniEnv->GetMethodID(clsITimpl, "getApplicationDirectory", "()Ljava/lang/String;");
    check_exception(status, "Cannot find getApplicationDirectory method");

    // getMasterWorkingDirectory method
    midTempDir = status->localJniEnv->GetMethodID(clsITimpl, "getTempDir", "()Ljava/lang/String;");
    check_exception(status, "Cannot find getMasterWorkingDirectory method");

    // RegisterCE method
    midRegisterCE = status->localJniEnv->GetMethodID(clsITimpl, "registerCoreElement", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;)V");
    check_exception(status, "Cannot find registerCoreElement");

    // Load stopIT
    midStopIT = status->localJniEnv->GetMethodID(clsITimpl, "stopIT", "(Z)V");
    check_exception(status, "Cannot find stopIT method.");

    debug_printf ("[BINDING-COMMONS] - @Init JNI Methods DONE\n");

    // Task OnFailure behaviour
    debug_printf ("[BINDING-COMMONS] - @Init JNI OnFailure Types\n");

    clsOnFailure = status->localJniEnv->FindClass("es/bsc/compss/types/annotations/parameter/OnFailure");
    check_exception(status, "Cannot find OnFailure Class");
    midOnFailureCon = status->localJniEnv->GetStaticMethodID(clsOnFailure, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/OnFailure;");
    check_exception(status, "Cannot find OnFailure constructor");

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

    JNIEnv* env = status->localJniEnv;

    // Allocate a local frame for this parameter (auto-cleans locals)
    if (env->PushLocalFrame(32) < 0) {
        // Out of memory
        return;
    }

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

    jobject jobjParVal = NULL;

    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM DATA_TYPE: %d\n", (enum datatype) parType);

    switch ( (enum datatype) parType) {
        case char_dt:
        case wchar_dt:
            jobjParVal = env->NewObject(clsCharacter, midCharCon, (jchar)*(char*)parVal);
            check_exception(status, "Cannot instantiate new char object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Char: %c\n", *(char*)parVal);
            break;
        case boolean_dt:
            jobjParVal = env->NewObject(clsBoolean, midBoolCon, (jboolean)*(int*)parVal);
            check_exception(status, "Cannot instantiate new boolean object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Bool: %d\n", *(int*)parVal);
            break;
        case short_dt:
            jobjParVal = env->NewObject(clsShort, midShortCon, (jshort)*(short*)parVal);
            check_exception(status, "Cannot instantiate new short object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Short: %hu\n", *(short*)parVal);
            break;
        case int_dt:
            jobjParVal = env->NewObject(clsInteger, midIntCon, (jint)*(int*)parVal);
            check_exception(status, "Cannot instantiate new int object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Int: %d\n", *(int*)parVal);
            break;
        case long_dt:
            jobjParVal = env->NewObject(clsLong, midLongCon, (jlong)*(long*)parVal);
            check_exception(status, "Cannot instantiate new long object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Long: %ld\n", *(long*)parVal);
            break;
        case longlong_dt:
        case float_dt:
            jobjParVal = env->NewObject(clsFloat, midFloatCon, (jfloat)*(float*)parVal);
            check_exception(status, "Cannot instantiate new float object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Float: %f\n", *(float*)parVal);
            break;
        case double_dt:
            jobjParVal = env->NewObject(clsDouble, midDoubleCon, (jdouble)*(double*)parVal);
            check_exception(status, "Cannot instantiate new double object");
            debug_printf ("[BINDING-COMMONS] - @process_param - Double: %f\n", *(double*)parVal);
            break;
        case file_dt:
            jobjParVal = env->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for file)");
            debug_printf ("[BINDING-COMMONS] - @process_param - File: %s\n", *(char **)parVal);
            break;
        case directory_dt:
            jobjParVal = env->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for directory)");
            debug_printf ("[BINDING-COMMONS] - @process_param - Directory: %s\n", *(char **)parVal);
            break;
        case external_stream_dt:
            jobjParVal = env->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for stream)");
            debug_printf ("[BINDING-COMMONS] - @process_param - External Stream: %s\n", *(char **)parVal);
            break;
        case external_psco_dt:
            jobjParVal = env->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for psco)");
            debug_printf ("[BINDING-COMMONS] - @process_param - Persistent: %s\n", *(char **)parVal);
            break;
        case string_dt:
            jobjParVal = env->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object");
            debug_printf ("[BINDING-COMMONS] - @process_param - String: %s\n", *(char **)parVal);
            break;
        case string_64_dt:
            jobjParVal = env->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object");
            debug_printf ("[BINDING-COMMONS] - @process_param - String: %s\n", *(char **)parVal);
            break;
        case binding_object_dt:
            jobjParVal = env->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for binding object)");
            debug_printf ("[BINDING-COMMONS] - @process_param - BindingObject: %s\n", *(char **)parVal);
            break;
        case collection_dt:
            jobjParVal = env->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for collection)");
            debug_printf ("[BINDING-COMMONS] - @process_param - Collection: %s\n", *(char **)parVal);
            break;
        case dict_collection_dt:
            jobjParVal = env-> NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for dictionary collection)");
            debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Dictionary Collection: %s\n", *(char **)parVal);
            break;
        case null_dt:
            jobjParVal = env-> NewStringUTF("NULL");
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
    env->SetObjectArrayElement(jobjOBJArr, pv, jobjParVal);
    env->SetObjectArrayElement(jobjOBJArr, pt, BYTE_CACHE[parType]);

    // Add param direction
    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM DIRECTION: %d\n", (enum direction) parDirect);
    env->SetObjectArrayElement(jobjOBJArr, pd, BYTE_CACHE[parDirect]);

    // Add param stream
    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM STD IO STREAM: %d\n", (enum io_stream) parIOStream);
    env->SetObjectArrayElement(jobjOBJArr, ps, BYTE_CACHE[parIOStream]);
    
    // Add param prefix
    debug_printf ("[BINDING-COMMONS] - @process_param - PREFIX: %s\n", *(char**)parPrefix);
    jstring jobjParPrefix = env->NewStringUTF(*(char**)parPrefix);
    env->SetObjectArrayElement(jobjOBJArr, pp, jobjParPrefix);

    debug_printf ("[BINDING-COMMONS] - @process_param - NAME: %s\n", *(char**)parName);
    jstring jobjParName = env->NewStringUTF(*(char**)parName);
    env->SetObjectArrayElement(jobjOBJArr, pn, jobjParName);

    debug_printf ("[BINDING-COMMONS] - @process_param - CONTENT TYPE: %s\n", *(char**)parConType);
    jstring jobConType = env->NewStringUTF(*(char**)parConType);
    env->SetObjectArrayElement(jobjOBJArr, pc, jobConType);

    debug_printf ("[BINDING-COMMONS] - @process_param - WEIGHT : %s\n", *(char**)parWeight);
    jstring jobjParWeight = env->NewStringUTF(*(char**)parWeight);
    env->SetObjectArrayElement(jobjOBJArr, pw, jobjParWeight);

    debug_printf ("[BINDING-COMMONS] - @process_param - KEEP RENAME : %d\n", parKeepRename);
    bool _KeepRename = false;
    if (parKeepRename != 0) _KeepRename = true;
	jobject jobjParKeepRename = env->NewObject(clsBoolean, midBoolCon, _KeepRename);
	check_exception(status, "Exception creating a new boolean for keep rename property");
	env->SetObjectArrayElement(jobjOBJArr, pkr, jobjParKeepRename);

    env->PopLocalFrame(NULL);
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


void JNI_WF_deregister(CompssWorkflow* self, bool deleteData) {
    debug_printf("[BINDING-COMMONS] - @JNI_WF_Deregister\n");
    JNIWorkflow* wf = (JNIWorkflow*) self;
    jboolean _deleteData     = deleteData     ? JNI_TRUE : JNI_FALSE;

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    
    JNIEnv* env = status->localJniEnv;

    env->CallVoidMethod(wf->jWorkflow, mid_wf_deregister, _deleteData);
    check_exception(status, "Workflow.deregister failed");
    env->DeleteGlobalRef(wf->jWorkflow);

    // Revoke thread access to JVM
    access_revoke(status);

}

void JNI_WF_openTaskGroup(CompssWorkflow* self, const char* groupName, bool implicitBarrier) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_openTaskGroup\n");

    jboolean _implicitBarrier     = implicitBarrier     ? JNI_TRUE : JNI_FALSE;

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jstring jGroup = env->NewStringUTF(groupName);
    env->CallVoidMethod(wf->jWorkflow, mid_wf_openTaskGroup, jGroup, _implicitBarrier);
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

static void JNI_WF_executeTaskCommon(
        CompssWorkflow* self,
        const char* signature,
        int onFailure,
        int timeout,
        int priority,
        int numNodes,
        int reduce,
        int reduceChunkSize,
        int replicated,
        int distributed,
        int hasTarget,
        int numReturns,
        int numParams,
        void** params)
{
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_executeTask - Processing task execution.\n");

    jboolean _priority     = priority     ? JNI_TRUE : JNI_FALSE;
    jboolean _reduce       = reduce       ? JNI_TRUE : JNI_FALSE;
    jboolean _replicated   = replicated   ? JNI_TRUE : JNI_FALSE;
    jboolean _distributed  = distributed  ? JNI_TRUE : JNI_FALSE;
    jboolean _hasTarget    = hasTarget    ? JNI_TRUE : JNI_FALSE;

    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jstring jSignature  = env->NewStringUTF(signature ? signature : "");

    jbyte jOnFailure = (jbyte) onFailure;

    jobject numReturnsInteger = env->NewObject(clsInteger, midIntCon, numReturns);
    check_exception(status, "Exception converting numReturns to integer");

    jobjectArray jobjOBJArr =
        (jobjectArray) env->NewObjectArray(numParams * NUM_FIELDS, clsObject, NULL);

    for (int i = 0; i < numParams; i++) {
        debug_printf("[BINDING-COMMONS] - @JNI_WF_executeTask - Processing parameter %d\n", i);
        process_param(status, params, i, jobjOBJArr);
    }

    jint taskId = env->CallIntMethod(
            wf->jWorkflow,
            mid_wf_execute,
            jSignature,
            jOnFailure,
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

    check_exception(status, "Exception received when calling executeTask");

    env->DeleteLocalRef(jobjOBJArr);
    env->DeleteLocalRef(numReturnsInteger);
    env->DeleteLocalRef(jSignature);

    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_WF_executeTask - Task processed.\n");
}

void JNI_WF_executeTask(CompssWorkflow* self, char* signature, int onFailure, int timeout, int priority, int numNodes, int reduce, int reduceChunkSize,
                        int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {
    debug_printf ("[BINDING-COMMONS] - @JNI_ExecuteTask - Processing task execution in bindings-common. \n");
    JNI_WF_executeTaskCommon(
        self,
        signature,
        onFailure,
        timeout,
        priority,
        numNodes,
        reduce,
        reduceChunkSize,
        replicated,
        distributed,
        hasTarget,
        numReturns,
        numParams,
        params);
    debug_printf ("[BINDING-COMMONS] - @JNI_WF_executeTask - Task processed.\n");
}


void JNI_WF_executeHttpTask(CompssWorkflow* self, char* signature, int onFailure, int timeout, int priority, int numNodes, int reduce,
                         int reduceChunkSize, int replicated, int distributed, int hasTarget, int numReturns, int numParams, void** params) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf ("[BINDING-COMMONS] - @JNI_WF_executeHttpTask - HTTP task execution in bindings-common. \n");
    JNI_WF_executeTaskCommon(
        self,
        signature,
        onFailure,
        timeout,
        priority,
        numNodes,
        reduce,
        reduceChunkSize,
        replicated,
        distributed,
        hasTarget,
        numReturns,
        numParams,
        params);
    debug_printf ("[BINDING-COMMONS] - @JNI_WF_executeHttpTask - HTTP Task processed.\n");
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


int JNI_WF_isFileAccessed(CompssWorkflow* self, char* fileName){
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_Accessed_File - Calling runtime isFileAccessed method  for %s  ...\n", fileName);
    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    // Format filename
	jstring jFilename = env->NewStringUTF(fileName);
	check_exception(status, "Error getting String UTF");

    // Perform operation
	jboolean is_accessed = (jboolean)env->CallBooleanMethod(wf->jWorkflow,
                                                                mid_wf_isFileAccessed,
                                                                jFilename);
    check_exception(status, "Error calling runtime isFileAccessed");
    env->DeleteLocalRef(jFilename);

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


void JNI_WF_openFile(CompssWorkflow* self, char* fileName, int mode, char** buf) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_openFile - Calling runtime OpenFile method  for %s and mode %d ...\n", fileName, mode);

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jbyte directionByte = (jbyte) mode;

    if (mode != null_dir){
        // Parse fileName
        jstring filename_str = env->NewStringUTF(fileName);
        check_exception(status, "Error getting String UTF");
        jstring jstr = (jstring)env->CallObjectMethod(wf->jWorkflow,
                                                        mid_wf_openFile,
                                                        filename_str,
                                                        directionByte);
        check_exception(status, "Exception calling runtime openFile");
        env->DeleteLocalRef(filename_str);

        // Parse output
        jboolean isCopy;
        const char* cstr = env->GetStringUTFChars(jstr, &isCopy);
        check_exception(status, "Exception getting String UTF");

        *buf = strdup(cstr);
        env->ReleaseStringUTFChars(jstr, cstr);
        env->DeleteLocalRef(jstr);
    }

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_WF_openFile - COMPSs filename: %s\n", *buf);
}


void JNI_WF_getFile(CompssWorkflow* self, char* fileName) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_getFile - Calling runtime getFile method...\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    // Format filename
	jstring jFilename = env->NewStringUTF(fileName);
	check_exception(status, "Error getting String UTF");

    // Perform operation
    env->CallVoidMethod(wf->jWorkflow,
                                mid_wf_getFile,
                                jFilename);
    check_exception(status, "Exception received when calling getFile");

    env->DeleteLocalRef(jFilename);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_WF_getFile - COMPSs filename: %s\n", fileName);
}


void JNI_WF_closeFile(CompssWorkflow* self, char* fileName, int mode) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_closeFile - Calling runtime closeFile method...\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    jbyte directionByte = (jbyte) mode;
    if (mode != null_dir){
        // Parse fileName
        jstring filename_str = env->NewStringUTF(fileName);
        check_exception(status, "Error getting String UTF");
        jstring jstr = (jstring)env->CallObjectMethod(wf->jWorkflow,
                                                        mid_wf_closeFile,
                                                        filename_str,
                                                        directionByte);
        check_exception(status, "Exception calling runtime closeFile");
        env->DeleteLocalRef(filename_str);
    }

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_WF_closeFile - COMPSs filename: %s\n", fileName);
}


bool JNI_WF_deleteFile(CompssWorkflow* self, char* fileName, int wait, int applicationDelete) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_deleteFile - Calling runtime deleteFile method...\n");
    debug_printf("[BINDING-COMMONS] - @JNI_WF_deleteFile - COMPSs filename: %s\n", fileName);

    // Local variables for JVM call
    jboolean _wait = wait? JNI_TRUE : JNI_FALSE;
    jboolean _applicationDelete = applicationDelete ? JNI_TRUE : JNI_FALSE;

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    // Format filename
	jstring jFilename = env->NewStringUTF(fileName);
	check_exception(status, "Error getting String UTF");

    // Perform operation
    jboolean res = env->CallBooleanMethod(wf->jWorkflow,
                                            mid_wf_deleteFile,
                                            jFilename,
                                            _wait,
                                            _applicationDelete);

    check_exception(status, "Exception received when calling deleteFile");
    env->DeleteLocalRef(jFilename);
    
    bool ret = 0;
    if ((bool) res) {
    	ret = 1;
    }

    // Revoke thread access to JVM
    access_revoke(status);
    debug_printf("[BINDING-COMMONS] - @JNI_WF_deleteFile - File erased with status: %i\n", (bool) res);
    return ret;
}


void JNI_WF_getDirectory(CompssWorkflow* self, char* dirName) {
    JNIWorkflow* wf = (JNIWorkflow*) self;
    debug_printf("[BINDING-COMMONS] - @JNI_WF_getDirectory - Calling runtime getDirectory method...\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    JNIEnv* env = status->localJniEnv;

    // Format filename
	jstring jFilename = env->NewStringUTF(dirName);
	check_exception(status, "Error getting String UTF");

    // Perform operation
    env->CallVoidMethod(wf->jWorkflow,
                                mid_wf_getDirectory,
                                jFilename);
    check_exception(status, "Exception received when calling getDirectory");

    env->DeleteLocalRef(jFilename);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_WF_getDirectory - COMPSs filename: %s\n", dirName);
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

        mid_wf_execute = env->GetMethodID(clsWorkflow, "executeTask", "(Ljava/lang/String;BIZIZIZZZLjava/lang/Integer;I[Ljava/lang/Object;)I");
        check_exception(status, "Cannot find executeTask");

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
        mid_wf_deregister = env->GetMethodID(clsWorkflow, "deregister", "(Z)V");
        check_exception(status, "Cannot find the Workflow.deregister  method");

        // Data operations
        mid_wf_getBindingObject = env->GetMethodID(clsWorkflow, "getBindingObject", "(Ljava/lang/String;)Ljava/lang/String;");
        check_exception(status, "Cannot find Workflow.getBindingObject");
        mid_wf_deleteBindingObject = env->GetMethodID(clsWorkflow, "deleteBindingObject", "(Ljava/lang/String;)Z");
        check_exception(status, "Cannot find Workflow.deleteBindingObject");
        mid_wf_isFileAccessed = env->GetMethodID(clsWorkflow,  "isFileAccessed", "(Ljava/lang/String;)Z");
        check_exception(status, "Cannot find Workflow.isFileAccessed");
        mid_wf_openFile = env->GetMethodID(clsWorkflow, "openFile", "(Ljava/lang/String;B)Ljava/lang/String;");
        check_exception(status, "Cannot find Workflow.openFile");
        mid_wf_getFile = env->GetMethodID(clsWorkflow, "getFile", "(Ljava/lang/String;)V");
        check_exception(status, "Cannot find Workflow.getFile");
        mid_wf_closeFile = env->GetMethodID(clsWorkflow, "closeFile", "(Ljava/lang/String;B)V");
        check_exception(status, "Cannot find Workflow.closeFile");
        mid_wf_deleteFile = env->GetMethodID(clsWorkflow, "deleteFile", "(Ljava/lang/String;ZZ)Z");
        check_exception(status, "Cannot find Workflow.deleteFile");
        mid_wf_getDirectory = env->GetMethodID(clsWorkflow, "getDirectory", "(Ljava/lang/String;)V");
        check_exception(status, "Cannot find Workflow.getDirectory");
    }

    // Wrap Java Workflow object into a C struct implementing the interface
    JNIWorkflow* jwf = (JNIWorkflow*) malloc(sizeof(JNIWorkflow));
    jwf->jWorkflow = env->NewGlobalRef(jWorkflowObj);

    // Revoke thread access to JVM
    access_revoke(status);

    jwf->base.getId = JNI_WF_getId;
    jwf->base.deregister = JNI_WF_deregister;
    jwf->base.openTaskGroup = JNI_WF_openTaskGroup;
    jwf->base.closeTaskGroup = JNI_WF_closeTaskGroup;
    jwf->base.executeTask = JNI_WF_executeTask;
    jwf->base.executeHttpTask = JNI_WF_executeHttpTask;
    jwf->base.cancelTaskGroup = JNI_WF_cancelTaskGroup;
    jwf->base.cancelApplicationTasks = JNI_WF_cancelApplicationTasks;
    jwf->base.noMoreTasks = JNI_WF_noMoreTasks;
    jwf->base.barrier = JNI_WF_barrier;
    jwf->base.barrierWithFlag = JNI_WF_barrierWithFlag;
    jwf->base.barrierGroup = JNI_WF_barrierGroup;
    jwf->base.snapshot = JNI_WF_snapshot;
    jwf->base.get_object = JNI_WF_getObject;
    jwf->base.delete_object = JNI_WF_deleteObject;
    jwf->base.is_file_accessed = JNI_WF_isFileAccessed;
    jwf->base.open_file = JNI_WF_openFile;
    jwf->base.get_file = JNI_WF_getFile;
    jwf->base.close_file = JNI_WF_closeFile;
    jwf->base.delete_file = JNI_WF_deleteFile;
    jwf->base.get_directory = JNI_WF_getDirectory;

    CompssWorkflow* wf = (CompssWorkflow*)jwf;
    long wf_id = wf->getId(wf);
    JNI_wf = wf;

    debug_printf ("[BINDING-COMMONS] - @JNI_RegisterWorkflow - Workflow registered with id %ld\n", wf_id);

    return wf;
}

// ******************************
// API functions
// ******************************


void JNI_On() {
    debug_printf ("[BINDING-COMMONS] - @JNI_On\n");

    // Initialise COMPSs env vars for debugging (from commons.h)
    debug_printf ("[BINDING-COMMONS] - @JNI_On - Initialising environment\n");
    init_env_vars();

    // Create the JVM instance
    debug_printf ("[BINDING-COMMONS] - @JNI_On - Creating the JVM\n");
    create_vm();
    
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

    // Init Byte object cache
    init_byte_object_cache(status);

    // Init basic JNI types
    init_basic_jni_types(status);

    // Init master types
    init_master_jni_types(status, clsITimpl);

    // Revoke thread access to JVM
    // debug_printf ("[BINDING-COMMONS] - @JNI_On - Revoke thread access to JVM\n");
    access_revoke(status);

}


void JNI_Off(int code) {
    debug_printf("[BINDING-COMMONS] - @JNI_Off\n");
    if (JNI_wf != NULL) {
        JNI_wf->noMoreTasks(JNI_wf);
    }
    
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
    destroy_vm();    

    // Delete environment
    debug_printf("[BINDING-COMMONS] - @Off - Removing environment\n");

    // End
    debug_printf("[BINDING-COMMONS] - @Off - End\n");
}


void JNI_read_command(char** command){
    // Do nothing
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


CompssInterface setup_JNI_runtime(){
    CompssInterface iface{};
    iface.On = JNI_On;
    iface.Off = JNI_Off;
    iface.read_command = JNI_read_command;

    iface.Get_AppDir = JNI_Get_AppDir;
    iface.Get_MasterWorkingDir = JNI_Get_MasterWorkingDir;
    
    iface.registerWorkflow = JNI_RegisterWorkflow;
    iface.RegisterCE = JNI_RegisterCE;

    return iface;
}
