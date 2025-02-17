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

#include <stdlib.h>
#include <jni.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>

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

JNIEnv* globalJniEnv;
JavaVM* globalJvm;
pthread_mutex_t globalJniAccessMutex;
jobject globalRuntime;

jmethodID midAppDir;                    /* ID of the getApplicationDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midTempDir;                   /* ID of the getTempDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midExecute;                   /* ID of the executeTask method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midExecuteNew;                /* ID of the executeTask method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midExecuteHttp;                /* ID of the executeTask method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midRegisterCE;                /* ID of the RegisterCE method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midEmitEvent;                 /* ID of the EmitEvent method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midCancelApplicationTasks;    /* ID of the CancelApplicationTasks method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midIsFileAccessed;            /* ID of the isFileAccessed method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midOpenFile;                  /* ID of the openFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midCloseFile;                 /* ID of the closeFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midDeleteFile;                /* ID of the deleteFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midGetFile;                   /* ID of the getFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midGetDirectory;              /* ID of the getDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midBarrier; 		            /* ID of the barrier method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midBarrierNew;                /* ID of the barrier method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midBarrierGroup;              /* ID of the barrierGroup method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midOpenTaskGroup;             /* ID of the openTaskGroup method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midCloseTaskGroup;            /* ID of the closeTaskGroup method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midCancelTaskGroup;            /* ID of the cancelTaskGroup method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midSnapshot; 		            /* ID of the snapshot method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midGetBindingObject;		    /* ID of the getBindingObject method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class  */
jmethodID midDeleteBindingObject; 	    /* ID of the deleteBindingObject method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class  */

jmethodID midGetNumberOfResources;      /* ID of the getNumberOfResources method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midRequestResources;          /* ID of the requestResources method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midFreeResources;             /* ID of the freeResources method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midNoMoreTasksIT;             /* ID of the noMoreTasks method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midStopIT;                    /* ID of the stopIT method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midSetWallClockLimit;			/* ID of the setWallClockLimit method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jclass clsOnFailure;
jmethodID midOnFailureCon;

jobject jobjParDirIN; 		        /* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */
jobject jobjParDirIN_DELETE;        /* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */
jobject jobjParDirOUT; 		        /* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */
jobject jobjParDirINOUT; 	        /* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */
jobject jobjParDirCONCURRENT; 		/* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */
jobject jobjParDirCOMMUTATIVE; 		/* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */

jobject jobjParStreamSTDIN;         /* Instance of the es.bsc.compss.types.annotations.parameter.StdIOStream class */
jobject jobjParStreamSTDOUT;        /* Instance of the es.bsc.compss.types.annotations.parameter.StdIOStream class */
jobject jobjParStreamSTDERR;        /* Instance of the es.bsc.compss.types.annotations.parameter.StdIOStream class */
jobject jobjParStreamUNSPECIFIED;   /* Instance of the es.bsc.compss.types.annotations.parameter.StdIOStream class */

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


/**
 * Initialises the JNI basic types.
 */
void init_basic_jni_types(ThreadStatus* status) {
    // Parameter classes
    debug_printf ("[BINDING-COMMONS] - @Init JNI Types\n");

    jclass clsLocal = status->localJniEnv->FindClass("java/lang/Object");
    check_exception(status, "Cannot find object class");
    clsObject = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot create global object class");
    midObjCon = status->localJniEnv->GetMethodID(clsObject, "<init>", "()V");
    check_exception(status, "Cannot find object constructor");

    clsLocal = status->localJniEnv->FindClass("java/lang/String");
    check_exception(status, "Cannot find string class");
    clsString = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot create global string class");
    midStrCon = status->localJniEnv->GetMethodID(clsString, "<init>", "(Ljava/lang/String;)V");
    check_exception(status, "Cannot find string constructor");

    clsLocal = status->localJniEnv->FindClass("java/lang/Character");
    check_exception(status, "Cannot find char class");
    clsCharacter = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot create global char class");
    midCharCon = status->localJniEnv->GetMethodID(clsCharacter, "<init>", "(C)V");
    check_exception(status, "Cannot find char constructor");

    clsLocal = status->localJniEnv->FindClass("java/lang/Boolean");
    check_exception(status, "Cannot find boolean class");
    clsBoolean = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot find boolean class");

    midBoolCon = status->localJniEnv->GetMethodID(clsBoolean, "<init>", "(Z)V");
    check_exception(status, "Cannot find boolean class");

    clsLocal = status->localJniEnv->FindClass("java/lang/Short");
    check_exception(status, "Cannot find boolean class");
    clsShort = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot find boolean class");

    midShortCon = status->localJniEnv->GetMethodID(clsShort, "<init>", "(S)V");
    check_exception(status, "Cannot find boolean constructor");

    clsLocal = status->localJniEnv->FindClass("java/lang/Integer");
    check_exception(status, "Cannot find Integer class");
    clsInteger = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot find Integer class");

    midIntCon = status->localJniEnv->GetMethodID(clsInteger, "<init>", "(I)V");
    check_exception(status, "Cannot find Integer constructor");

    clsLocal = status->localJniEnv->FindClass("java/lang/Long");
    check_exception(status, "Cannot find Long class");
    clsLong = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot find Long class");

    midLongCon = status->localJniEnv->GetMethodID(clsLong, "<init>", "(J)V");
    check_exception(status, "Cannot find Long constructor");

    clsLocal = status->localJniEnv->FindClass("java/lang/Float");
    check_exception(status, "Cannot find Float Class");
    clsFloat = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot find Float Class");

    midFloatCon = status->localJniEnv->GetMethodID(clsFloat, "<init>", "(F)V");
    check_exception(status, "Cannot find Float Constructor");

    clsLocal = status->localJniEnv->FindClass("java/lang/Double");
    check_exception(status, "Cannot find Double Class");
    clsDouble = (jclass)status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot find Double Class");

    midDoubleCon = status->localJniEnv->GetMethodID(clsDouble, "<init>", "(D)V");
    check_exception(status, "Cannot find Double Constructor");

    debug_printf ("[BINDING-COMMONS] - @Init JNI Types DONE\n");
}


/**
 * Initialises the COMPSs related types.
 */
void init_master_jni_types(ThreadStatus* status, jclass clsITimpl) {
    debug_printf ("[BINDING-COMMONS] - @Init JNI Master\n");


    // JNI API method calls
    debug_printf ("[BINDING-COMMONS] - @Init JNI Methods\n");

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

    // barrier method
    midBarrier = status->localJniEnv->GetMethodID(clsITimpl, "barrier", "(Ljava/lang/Long;)V");
    check_exception(status, "Cannot find barrier");

    // barrier method (with no more tasks flag)
    midBarrierNew = status->localJniEnv->GetMethodID(clsITimpl, "barrier", "(Ljava/lang/Long;Z)V");
    check_exception(status, "Cannot find barrier new");

    // barrierGroup method
    midBarrierGroup = status->localJniEnv->GetMethodID(clsITimpl, "barrierGroup", "(Ljava/lang/Long;Ljava/lang/String;)V");
    check_exception(status, "Cannot find barrierGroup");

    // openTaskGroup method
    midOpenTaskGroup = status->localJniEnv->GetMethodID(clsITimpl, "openTaskGroup", "(Ljava/lang/String;ZLjava/lang/Long;)V");
    check_exception(status, "Cannot find openTaskGroup");

    // closeTaskGroup method
    midCloseTaskGroup = status->localJniEnv->GetMethodID(clsITimpl, "closeTaskGroup", "(Ljava/lang/String;Ljava/lang/Long;)V");
    check_exception(status, "Cannot find closeTaskGroup");

    // closeTaskGroup method
    midCancelTaskGroup = status->localJniEnv->GetMethodID(clsITimpl, "cancelTaskGroup", "(Ljava/lang/String;Ljava/lang/Long;)V");
    check_exception(status, "Cannot find cancelTaskGroup");

    // snapshot method
    midSnapshot = status->localJniEnv->GetMethodID(clsITimpl, "snapshot", "(Ljava/lang/Long;)V");
    check_exception(status, "Cannot find snapshot");

    // EmitEvent method
    midEmitEvent = status->localJniEnv->GetMethodID(clsITimpl, "emitEvent", "(IJ)V");
    check_exception(status, "Cannot find emitEvent");

    // CancelApplicationTasks method
    midCancelApplicationTasks = status->localJniEnv->GetMethodID(clsITimpl, "cancelApplicationTasks", "(Ljava/lang/Long;)V");
    check_exception(status, "Cannot find cancelApplicationTasks");

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

    // deleteFile method
    midDeleteBindingObject = status->localJniEnv->GetMethodID(clsITimpl, "deleteBindingObject", "(Ljava/lang/Long;Ljava/lang/String;)Z");
    check_exception(status, "Cannot find deleteBindingObject");

    // openFile method
    midGetBindingObject = status->localJniEnv->GetMethodID(clsITimpl, "getBindingObject", "(Ljava/lang/Long;Ljava/lang/String;)Ljava/lang/String;");
    check_exception(status, "Cannot find getBindingObject");

    // getNumberOfResources method
    midGetNumberOfResources = status->localJniEnv->GetMethodID(clsITimpl, "getNumberOfResources", "()I");
    check_exception(status, "Cannot find getNumberOfResources");

    // requestResourcesCreation method
    midRequestResources = status->localJniEnv->GetMethodID(clsITimpl, "requestResources", "(Ljava/lang/Long;ILjava/lang/String;)V");
    check_exception(status, "Cannot find requestResources");

    // requestResourcesDestruction method
    midFreeResources = status->localJniEnv->GetMethodID(clsITimpl, "freeResources", "(Ljava/lang/Long;ILjava/lang/String;)V");
    check_exception(status, "Cannot find freeResources");

    // Load NoMoreTasks
    midNoMoreTasksIT = status->localJniEnv->GetMethodID(clsITimpl, "noMoreTasks", "(Ljava/lang/Long;)V");
    check_exception(status, "Cannot find noMoreTasks method.");

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
    debug_printf ("[BINDING-COMMONS] - @Init JNI Direction Types\n");

    jclass clsParDir; 		    /* es.bsc.compss.types.annotations.parameter.Direction class */
    jmethodID midParDirCon; 	/* ID of the es.bsc.compss.types.annotations.parameter.Direction class constructor method */

    clsParDir = status->localJniEnv->FindClass("es/bsc/compss/types/annotations/parameter/Direction");
    check_exception(status, "Cannot find Direction Class");
    midParDirCon = status->localJniEnv->GetStaticMethodID(clsParDir, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/Direction;");
    check_exception(status, "Cannot find Direction constructor");

    jobject objLocal = status->localJniEnv->CallStaticObjectMethod(clsParDir, midParDirCon, status->localJniEnv->NewStringUTF("IN"));
    check_exception(status, "Cannot retrieve Direction.IN object");
    jobjParDirIN = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for Direction.IN object");

    objLocal = status->localJniEnv->CallStaticObjectMethod(clsParDir, midParDirCon, status->localJniEnv->NewStringUTF("IN_DELETE"));
    check_exception(status, "Cannot retrieve Direction.IN_DELETE object");
    jobjParDirIN_DELETE = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for Direction.IN_DELETE object");

    objLocal =  status->localJniEnv->CallStaticObjectMethod(clsParDir, midParDirCon, status->localJniEnv->NewStringUTF("OUT"));
    check_exception(status, "Cannot retrieve Direction.OUT object");
    jobjParDirOUT = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for Direction.OUT object");

    objLocal = status->localJniEnv->CallStaticObjectMethod(clsParDir, midParDirCon, status->localJniEnv->NewStringUTF("INOUT"));
    check_exception(status, "Cannot retrieve Direction.INOUT object");
    jobjParDirINOUT = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for Direction.INOUT object");

    objLocal =  status->localJniEnv->CallStaticObjectMethod(clsParDir, midParDirCon, status->localJniEnv->NewStringUTF("CONCURRENT"));
    check_exception(status, "Cannot retrieve Direction.CONCURRENT object");
    jobjParDirCONCURRENT = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for Direction.CONCURRENT object");

    objLocal =  status->localJniEnv->CallStaticObjectMethod(clsParDir, midParDirCon, status->localJniEnv->NewStringUTF("COMMUTATIVE"));
    check_exception(status, "Cannot retrieve Direction.COMMUTATIVE object");
    jobjParDirCOMMUTATIVE = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for Direction.COMMUTATIVE object");

    debug_printf ("[BINDING-COMMONS] - @Init JNI Direction Types DONE\n");


    // Parameter streams
    debug_printf ("[BINDING-COMMONS] - @Init JNI Stream Types\n");

    jclass clsParStream;        /* es.bsc.compss.types.annotations.parameter.StdIOStream class */
    jmethodID midParStreamCon;  /* es.bsc.compss.types.annotations.parameter.StdIOStream class constructor method */

    clsParStream = status->localJniEnv->FindClass("es/bsc/compss/types/annotations/parameter/StdIOStream");
    check_exception(status, "Cannot find StdIOStream class");
    midParStreamCon = status->localJniEnv->GetStaticMethodID(clsParStream, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/StdIOStream;");
    check_exception(status, "Cannot find StdIOStream constructor");

    objLocal = status->localJniEnv->CallStaticObjectMethod(clsParStream, midParStreamCon, status->localJniEnv->NewStringUTF("STDIN"));
    check_exception(status, "Cannot retrieve StdIOStream.STDIN object");
    jobjParStreamSTDIN = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for StdIOStream.STDIN object");

    objLocal = status->localJniEnv->CallStaticObjectMethod(clsParStream, midParStreamCon, status->localJniEnv->NewStringUTF("STDOUT"));
    check_exception(status, "Cannot retrieve StdIOStream.STDOUT object");
    jobjParStreamSTDOUT = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for StdIOStream.STDOUT object");

    objLocal = status->localJniEnv->CallStaticObjectMethod(clsParStream, midParStreamCon, status->localJniEnv->NewStringUTF("STDERR"));
    check_exception(status, "Cannot retrieve StdIOStream.STDERR object");
    jobjParStreamSTDERR = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for StdIOStream.STDERR object");

    objLocal = status->localJniEnv->CallStaticObjectMethod(clsParStream, midParStreamCon, status->localJniEnv->NewStringUTF("UNSPECIFIED"));
    check_exception(status, "Cannot retrieve StdIOStream.UNSPECIFIED object");
    jobjParStreamUNSPECIFIED = (jobject)status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create global reference for StdIOStream.UNSPECIFIED object");

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
    void *parWeight	=           params[pw];
    int parKeepRename   = *(int*)   params[pkr];

    jclass clsParType = NULL; /* es.bsc.compss.types.annotations.parameter.DataType class */
    clsParType = status->localJniEnv->FindClass("es/bsc/compss/types/annotations/parameter/DataType");
    check_exception(status, "Cannot load DataType class");

    jmethodID midParTypeCon = NULL; /* ID of the es.bsc.compss.api.COMPSsRuntime$DataType class constructor method */
    midParTypeCon = status->localJniEnv->GetStaticMethodID(clsParType, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/DataType;");
    check_exception(status, "Cannot get DataType constructor");

    jobject jobjParType = NULL;
    jobject jobjParVal = NULL;

    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM DATA_TYPE: %d\n", (enum datatype) parType);

    switch ( (enum datatype) parType) {
        case char_dt:
        case wchar_dt:
            jobjParVal = status->localJniEnv->NewObject(clsCharacter, midCharCon, (jchar)*(char*)parVal);
            check_exception(status, "Cannot instantiate new char object");

            debug_printf ("[BINDING-COMMONS] - @process_param - Char: %c\n", *(char*)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("CHAR_T"));
            check_exception(status, "Exception calling char constructor");
            break;
        case boolean_dt:
            jobjParVal = status->localJniEnv->NewObject(clsBoolean, midBoolCon, (jboolean)*(int*)parVal);
            check_exception(status, "Cannot instantiate new boolean object");

            debug_printf ("[BINDING-COMMONS] - @process_param - Bool: %d\n", *(int*)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("BOOLEAN_T"));
            check_exception(status, "Exception calling boolean constructor");
            break;
        case short_dt:
            jobjParVal = status->localJniEnv->NewObject(clsShort, midShortCon, (jshort)*(short*)parVal);
            check_exception(status, "Cannot instantiate new short object");

            debug_printf ("[BINDING-COMMONS] - @process_param - Short: %hu\n", *(short*)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("SHORT_T"));
            check_exception(status, "Exception calling short constructor");
            break;
        case int_dt:
            jobjParVal = status->localJniEnv->NewObject(clsInteger, midIntCon, (jint)*(int*)parVal);
            check_exception(status, "Cannot instantiate new int object");

            debug_printf ("[BINDING-COMMONS] - @process_param - Int: %d\n", *(int*)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("INT_T"));
            check_exception(status, "Exception calling int constructor");
            break;
        case long_dt:
            jobjParVal = status->localJniEnv->NewObject(clsLong, midLongCon, (jlong)*(long*)parVal);
            check_exception(status, "Cannot instantiate new long object");

            debug_printf ("[BINDING-COMMONS] - @process_param - Long: %ld\n", *(long*)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("LONG_T"));
            check_exception(status, "Exception calling long constructor");
            break;
        case longlong_dt:
        case float_dt:
            jobjParVal = status->localJniEnv->NewObject(clsFloat, midFloatCon, (jfloat)*(float*)parVal);
            check_exception(status, "Cannot instantiate new float object");

            debug_printf ("[BINDING-COMMONS] - @process_param - Float: %f\n", *(float*)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("FLOAT_T"));
            check_exception(status, "Exception calling float constructor");
            break;
        case double_dt:
            jobjParVal = status->localJniEnv->NewObject(clsDouble, midDoubleCon, (jdouble)*(double*)parVal);
            check_exception(status, "Cannot instantiate new double object");

            debug_printf ("[BINDING-COMMONS] - @process_param - Double: %f\n", *(double*)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("DOUBLE_T"));
            check_exception(status, "Exception calling double constructor");
            break;
        case file_dt:
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for file)");

            debug_printf ("[BINDING-COMMONS] - @process_param - File: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("FILE_T"));
            check_exception(status, "Exception calling string constructor (for file)");
            break;
        case directory_dt:
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for directory)");

            debug_printf ("[BINDING-COMMONS] - @process_param - Directory: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("DIRECTORY_T"));
            check_exception(status, "Exception calling string constructor (for directory)");
            break;
        case external_stream_dt:
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for stream)");

            debug_printf ("[BINDING-COMMONS] - @process_param - External Stream: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("EXTERNAL_STREAM_T"));
            check_exception(status, "Exception calling string constructor (for stream)");
            break;
        case external_psco_dt:
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for psco)");
            debug_printf ("[BINDING-COMMONS] - @process_param - Persistent: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("EXTERNAL_PSCO_T"));
            check_exception(status, "Exception calling string constructor (for psco)");
            break;
        case string_dt:
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object");

            debug_printf ("[BINDING-COMMONS] - @process_param - String: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("STRING_T"));
            check_exception(status, "Exception calling string constructor");
            break;
        case string_64_dt:
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object");

            debug_printf ("[BINDING-COMMONS] - @process_param - String: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("STRING_64_T"));
            check_exception(status, "Exception calling string constructor");
            break;
        case binding_object_dt:
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for binding object)");

            debug_printf ("[BINDING-COMMONS] - @process_param - BindingObject: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("BINDING_OBJECT_T"));
            check_exception(status, "Exception calling string constructor (for binding object)");
            break;
        case collection_dt:
            jobjParVal = status->localJniEnv->NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for collection)");

            debug_printf ("[BINDING-COMMONS] - @process_param - Collection: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("COLLECTION_T"));
            check_exception(status, "Exception calling string constructor (for collection)");
            break;
        case dict_collection_dt:
            jobjParVal = globalJniEnv -> NewStringUTF(*(char **)parVal);
            check_exception(status, "Cannot instantiate new string object (for dictionary collection)");

            debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Dictionary Collection: %s\n", *(char **)parVal);

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("DICT_COLLECTION_T"));
            check_exception(status, "Exception calling string constructor (for dictionary collection)");
            break;
        case null_dt:
            jobjParVal = globalJniEnv -> NewStringUTF("NULL");
            check_exception(status, "Cannot instantiate new null object");

            debug_printf ("[BINDING-COMMONS] - @process_param - Null: NULL\n");

            jobjParType = status->localJniEnv->CallStaticObjectMethod(clsParType, midParTypeCon, status->localJniEnv->NewStringUTF("NULL_T"));
            check_exception(status, "Exception calling null constructor");
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
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirIN);
            break;
        case out_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirOUT);
            break;
        case inout_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirINOUT);
            break;
        case concurrent_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirCONCURRENT);
            break;
        case commutative_dir:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirCOMMUTATIVE);
            break;
        case in_delete_dir:
        	status->localJniEnv->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirIN_DELETE);
        	break;
        default:
            break;
    }

    // Add param stream
    debug_printf ("[BINDING-COMMONS] - @process_param - ENUM STD IO STREAM: %d\n", (enum io_stream) parIOStream);
    switch ((enum io_stream) parIOStream) {
        case STD_IN:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamSTDIN);
            break;
        case STD_OUT:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamSTDOUT);
            break;
        case STD_ERR:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamSTDERR);
            break;
        default:
            status->localJniEnv->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamUNSPECIFIED);
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
}


void JNI_Off(int code) {
    debug_printf("[BINDING-COMMONS] - @JNI_Off\n");

    // Request thread access to JVM
    // debug_printf ("[BINDING-COMMONS] - @JNI_Off - Request thread access to JVM\n");
    ThreadStatus* status = access_request();

    // Create fake app Id (id = 0)
    jobject objLocal = status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) 0);
    check_exception(status, "Cannot instantiate application Id");
    jobject fakeAppId = (jobject) status->localJniEnv->NewGlobalRef(objLocal);
    check_exception(status, "Cannot create application Id");

    // Call noMoreTasks
    debug_printf("[BINDING-COMMONS] - @Off - Waiting to end tasks\n");
    status->localJniEnv->CallVoidMethod(globalRuntime, midNoMoreTasksIT, fakeAppId, "TRUE");
    check_exception(status, "Exception received when calling noMoreTasks.");

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


void JNI_Cancel_Application_Tasks(long appId) {
    debug_printf ("[BINDING-COMMONS] - @JNI_Cancel_Application_Tasks\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midCancelApplicationTasks,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId));
    check_exception(status, "Exception received when calling cancelApplicationTasks");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf ("[BINDING-COMMONS] - @JNI_Cancel_Application_Tasks - Tasks cancelled\n");
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
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
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
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
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
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
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
                                                                status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
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
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                                        filename_str,
                                                        jobjParDirIN);
            break;
        case out_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                                        filename_str,
                                                        jobjParDirOUT);
            break;
        case inout_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                                        filename_str,
                                                        jobjParDirINOUT);
            break;
        case concurrent_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                                        filename_str,
                                                        jobjParDirCONCURRENT);
            break;
        case commutative_dir:
            jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midOpenFile,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                                        filename_str,
                                                        jobjParDirCOMMUTATIVE);
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
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        jobjParDirIN);
            break;
        case out_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        jobjParDirOUT);
            break;
        case inout_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        jobjParDirINOUT);
            break;
        case concurrent_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        jobjParDirCONCURRENT);
            break;
        case commutative_dir:
            status->localJniEnv->CallVoidMethod(globalRuntime,
                                        midCloseFile,
                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                        status->localJniEnv->NewStringUTF(fileName),
                                        jobjParDirCOMMUTATIVE);
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
                                            status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
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
                                status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
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
                                status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                status->localJniEnv->NewStringUTF(dirName));
    check_exception(status, "Exception received when calling getDirectory");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Get_Directory - COMPSs directory: %s\n", dirName);
}

void JNI_Get_Object(long appId, char* fileName, char** buf) {
    debug_printf("[BINDING-COMMONS] - @JNI_Get_Object - Calling runtime getObject method...\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    jstring jstr = (jstring)status->localJniEnv->CallObjectMethod(globalRuntime,
                                                        midGetBindingObject,
                                                        status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                                        status->localJniEnv->NewStringUTF(fileName));
    check_exception(status, "Exception received when calling getObject");

    // Parse output
    jboolean isCopy;
    const char* cstr = status->localJniEnv->GetStringUTFChars(jstr, &isCopy);
    *buf = strdup(cstr);
    status->localJniEnv->ReleaseStringUTFChars(jstr, cstr);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Get_Object - COMPSs data id: %s\n", *buf);
}


void JNI_Delete_Object(long appId, char* fileName, int** buf) {
    debug_printf("[BINDING-COMMONS] - @JNI_Delete_Object - Calling runtime deleteObject method...\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    jboolean res = status->localJniEnv->CallBooleanMethod(globalRuntime,
                                                midDeleteBindingObject,
                                                status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                                                status->localJniEnv->NewStringUTF(fileName));
    check_exception(status, "Exception received when calling deleteObject");
    *buf = (int*) &res;

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Delete_Binding_Object - COMPSs obj: %s\n", fileName);
}


void JNI_Barrier(long appId) {
	debug_printf("[BINDING-COMMONS] - @JNI_Barrier - Waiting tasks for APP id: %lu\n", appId);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
	status->localJniEnv->CallVoidMethod(globalRuntime,
	                          midBarrier,
	                          status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId));
    check_exception(status, "Exception received when calling barrier");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - APP id: %lu\n", appId);
}


void JNI_BarrierNew(long appId, int noMoreTasks) {
    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - Waiting tasks for APP id: %lu\n", appId);

    // Local variables for JVM call
    bool _noMoreTasks = false;
    if (noMoreTasks != 0) _noMoreTasks = true;
    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - noMoreTasks: %s\n", _noMoreTasks ? "true":"false");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midBarrierNew,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                              _noMoreTasks);
    check_exception(status, "Exception received when calling barrierNew");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_Barrier - APP id: %lu\n", appId);
}


void JNI_BarrierGroup(long appId, char* groupName, char** exceptionMessage) {
    debug_printf("[BINDING-COMMONS] - @JNI_BarrierGroup - COMPSs group name: %s\n", groupName);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midBarrierGroup,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
                              status->localJniEnv->NewStringUTF(groupName));
    check_and_get_compss_exception(status, exceptionMessage);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_BarrierGroup - Barrier ended for COMPSs group name: %s\n", groupName);
}


void JNI_OpenTaskGroup(char* groupName, int implicitBarrier, long appId){
    debug_printf("[BINDING-COMMONS] - @JNI_OpenTaskGroup - Opening task group...\n");

    // Local variables for JVM call
    bool _implicitBarrier = false;
    if (implicitBarrier != 0) _implicitBarrier = true;
    debug_printf("[BINDING-COMMONS] - @JNI_OpenTaskGroup - implicit barrier: %s\n", _implicitBarrier ? "true":"false");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midOpenTaskGroup,
                              status->localJniEnv->NewStringUTF(groupName),
                              _implicitBarrier,
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId));
    check_exception(status, "Exception received when calling openTaskGroup");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_OpenTaskGroup - COMPSs group name: %s\n", groupName);
}


void JNI_CloseTaskGroup(char* groupName, long appId){
    debug_printf("[BINDING-COMMONS] - @JNI_CloseTaskGroup - COMPSs group name: %s\n", groupName);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midCloseTaskGroup,
                              status->localJniEnv->NewStringUTF(groupName),
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId));
    check_exception(status, "Exception received when calling closeTaskGroup");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_CloseTaskGroup - Task group %s closed.\n", groupName);
}

void JNI_CancelTaskGroup(char* groupName, long appId, char** exceptionMessage){
    debug_printf("[BINDING-COMMONS] - @JNI_CancelTaskGroup - COMPSs group name: %s\n", groupName);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallVoidMethod(globalRuntime,
                              midCancelTaskGroup,
                              status->localJniEnv->NewStringUTF(groupName),
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId));
    check_and_get_compss_exception(status, exceptionMessage);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_CancelTaskGroup - Task group %s canceled.\n", groupName);
}

void JNI_Snapshot(long appId) {
	debug_printf("[BINDING-COMMONS] - @JNI_Snapshot - Snapshot for APP id: %lu\n", appId);

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
	status->localJniEnv->CallVoidMethod(globalRuntime,
	                          midSnapshot,
	                          status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId));
    check_exception(status, "Exception received when calling snapshot");

    // Revoke thread access to JVM
    access_revoke(status);

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
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
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
                              status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
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
			status->localJniEnv->NewObject(clsLong, midLongCon, (jlong) appId),
			wcl, _stop);
	check_exception(status, "Exception received when calling setWallClockLimit");

	// Revoke thread access to JVM
	access_revoke(status);
}
