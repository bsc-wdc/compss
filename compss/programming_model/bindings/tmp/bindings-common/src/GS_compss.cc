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

#include <stdlib.h>
#include <jni.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>

#include "GS_compss.h"
#include "param_metadata.h"
#include "BindingDataManager.h"

using namespace std;
const int NUM_FIELDS = 6;

JNIEnv *m_env;
jobject jobjIT;
jclass clsITimpl;
JavaVM * m_jvm;

pthread_mutex_t mtx;

jobject appId;

jmethodID midAppDir;                /* ID of the getApplicationDirectory method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midExecute;               /* ID of the executeTask method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midExecuteNew;            /* ID of the executeTask method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midRegisterCE;            /* ID of the RegisterCE method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midEmitEvent;             /* ID of the EmitEvent method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midOpenFile;              /* ID of the openFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midCloseFile;             /* ID of the closeFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midDeleteFile;            /* ID of the deleteFile method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midBarrier; 		        /* ID of the barrier method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */
jmethodID midBarrierNew;            /* ID of the barrier method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class */

jmethodID midgetBindingObject;		/* ID of the getBindingObject method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class  */
jmethodID midDeleteBindingObject; 	/* ID of the deleteBindingObject method in the es.bsc.compss.api.impl.COMPSsRuntimeImpl class  */

jobject jobjParDirIN; 		        /* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */
jobject jobjParDirINOUT; 	        /* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */
jobject jobjParDirOUT; 		        /* Instance of the es.bsc.compss.types.annotations.parameter.Direction class */

jobject jobjParStreamSTDIN;         /* Instance of the es.bsc.compss.types.annotations.parameter.Stream class */
jobject jobjParStreamSTDOUT;        /* Instance of the es.bsc.compss.types.annotations.parameter.Stream class */
jobject jobjParStreamSTDERR;        /* Instance of the es.bsc.compss.types.annotations.parameter.Stream class */
jobject jobjParStreamUNSPECIFIED;   /* Instance of the es.bsc.compss.types.annotations.parameter.Stream class */

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

void init_basic_jni_types() {
    // Parameter classes
    debug_printf ("[BINDING-COMMONS]  -  @Init JNI Types\n");

    jclass clsLocal = m_env->FindClass("java/lang/Object");
    check_and_treat_exception(m_env, "Looking for object class");
    clsObject = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for object class");

    midObjCon = m_env->GetMethodID(clsObject, "<init>", "()V");
    check_and_treat_exception(m_env, "Looking for object constructor");
    clsLocal = m_env->FindClass("java/lang/String");
    check_and_treat_exception(m_env, "Looking for string class");
    clsString = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for string class");
    midStrCon = m_env->GetMethodID(clsString, "<init>", "(Ljava/lang/String;)V");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }
    clsLocal = m_env->FindClass("java/lang/Character");
    check_and_treat_exception(m_env, "Looking for string class");
    clsCharacter = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for string class");

    midCharCon = m_env->GetMethodID(clsCharacter, "<init>", "(C)V");
    check_and_treat_exception(m_env, "Looking for string constructor");

    clsLocal = m_env->FindClass("java/lang/Boolean");
    check_and_treat_exception(m_env, "Looking for boolean class");
    clsBoolean = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for boolean class");

    midBoolCon = m_env->GetMethodID(clsBoolean, "<init>", "(Z)V");
    check_and_treat_exception(m_env, "Looking for boolean class");

    clsLocal = m_env->FindClass("java/lang/Short");
    check_and_treat_exception(m_env, "Looking for boolean class");
    clsShort = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for boolean class");

    midShortCon = m_env->GetMethodID(clsShort, "<init>", "(S)V");
    check_and_treat_exception(m_env, "Looking for boolean constructor");

    clsLocal = m_env->FindClass("java/lang/Integer");
    check_and_treat_exception(m_env, "Looking for Integer class");
    clsInteger = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for Integer class");

    midIntCon = m_env->GetMethodID(clsInteger, "<init>", "(I)V");
    check_and_treat_exception(m_env, "Looking for Integer constructor");

    clsLocal = m_env->FindClass("java/lang/Long");
    check_and_treat_exception(m_env, "Looking for Long class");
    clsLong = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for Long class");

    midLongCon = m_env->GetMethodID(clsLong, "<init>", "(J)V");
    check_and_treat_exception(m_env, "Looking for Long constructor");

    clsLocal = m_env->FindClass("java/lang/Float");
    check_and_treat_exception(m_env, "Looking for Float Class");
    clsFloat = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for Float Class");

    midFloatCon = m_env->GetMethodID(clsFloat, "<init>", "(F)V");
    check_and_treat_exception(m_env, "Looking for Float Constructor");

    clsLocal = m_env->FindClass("java/lang/Double");
    check_and_treat_exception(m_env, "Looking for Double Class");
    clsDouble = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Looking for Double Class");

    midDoubleCon = m_env->GetMethodID(clsDouble, "<init>", "(D)V");
    check_and_treat_exception(m_env, "Looking for Double Constructor");

    debug_printf ("[BINDING_COMMONS]  -  @Init DONE\n");
}


void init_master_jni_types() {
    jclass clsParDir; 		    /* es.bsc.compss.types.annotations.parameter.Direction class */
    jmethodID midParDirCon; 	/* ID of the es.bsc.compss.types.annotations.parameter.Direction class constructor method */
    jclass clsParStream;        /* es.bsc.compss.types.annotations.parameter.Stream class */
    jmethodID midParStreamCon;  /* es.bsc.compss.types.annotations.parameter.Stream class constructor method */

    debug_printf ("[BINDING-COMMONS]  -  @Init JNI Methods\n");

    // getApplicationDirectory method
    midAppDir = m_env->GetMethodID(clsITimpl, "getApplicationDirectory", "()Ljava/lang/String;");
    check_and_treat_exception(m_env, "Looking for getApplicationDirectory method");

    // executeTask method - Deprecated
    midExecute = m_env->GetMethodID(clsITimpl, "executeTask", "(Ljava/lang/Long;ZLjava/lang/String;Ljava/lang/String;Ljava/lang/String;ZIZZZLjava/lang/Integer;I[Ljava/lang/Object;)I");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // executeTask New method
    midExecuteNew = m_env->GetMethodID(clsITimpl, "executeTask", "(Ljava/lang/Long;Ljava/lang/String;ZIZZZLjava/lang/Integer;I[Ljava/lang/Object;)I");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // barrier method
    midBarrier = m_env->GetMethodID(clsITimpl, "barrier", "(Ljava/lang/Long;)V");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // barrier method (with no more tasks flag)
    midBarrierNew = m_env->GetMethodID(clsITimpl, "barrier", "(Ljava/lang/Long;Z)V");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // EmitEvent method
    midEmitEvent = m_env->GetMethodID(clsITimpl, "emitEvent", "(IJ)V");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // RegisterCE method
    midRegisterCE = m_env->GetMethodID(clsITimpl, "registerCoreElement", "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;[Ljava/lang/String;)V");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // openFile method
    midOpenFile = m_env->GetMethodID(clsITimpl, "openFile", "(Ljava/lang/String;Les/bsc/compss/types/annotations/parameter/Direction;)Ljava/lang/String;");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // closeFile method
    midCloseFile = m_env->GetMethodID(clsITimpl, "closeFile", "(Ljava/lang/String;Les/bsc/compss/types/annotations/parameter/Direction;)V");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // deleteFile method
    midDeleteFile = m_env->GetMethodID(clsITimpl, "deleteFile", "(Ljava/lang/String;)Z");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    // deleteFile method
    midDeleteBindingObject = m_env->GetMethodID(clsITimpl, "deleteBindingObject", "(Ljava/lang/String;)Z");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }
    // openFile method
    midgetBindingObject = m_env->GetMethodID(clsITimpl, "getBindingObject", "(Ljava/lang/String;)Ljava/lang/String;");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }


    // PARAMETER DIRECTIONS

    debug_printf ("[BINDING_COMMONS]  -  @Init JNI Direction Types\n");

    clsParDir = m_env->FindClass("es/bsc/compss/types/annotations/parameter/Direction");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }
    midParDirCon = m_env->GetStaticMethodID(clsParDir, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/Direction;");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }

    jobject objLocal = m_env->CallStaticObjectMethod(clsParDir, midParDirCon, m_env->NewStringUTF("IN"));
    check_and_treat_exception(m_env, "Error getting Direction.IN object");
    jobjParDirIN = (jobject)m_env->NewGlobalRef(objLocal);
    check_and_treat_exception(m_env, "Error getting Direction.IN object");

    objLocal = m_env->CallStaticObjectMethod(clsParDir, midParDirCon, m_env->NewStringUTF("INOUT"));
    check_and_treat_exception(m_env, "Error getting Direction.INOUT object");
    jobjParDirINOUT = (jobject)m_env->NewGlobalRef(objLocal);
    check_and_treat_exception(m_env, "Error getting Direction.INOUT object");

    objLocal =  m_env->CallStaticObjectMethod(clsParDir, midParDirCon, m_env->NewStringUTF("OUT"));
    check_and_treat_exception(m_env, "Error getting Direction.OUT object");
    jobjParDirOUT = (jobject)m_env->NewGlobalRef(objLocal);

    // Parameter streams

    debug_printf ("[BINDING_COMMONS]  -  @Init JNI Stream Types\n");

    clsParStream = m_env->FindClass("es/bsc/compss/types/annotations/parameter/Stream");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }
    midParStreamCon = m_env->GetStaticMethodID(clsParStream, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/Stream;");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        exit(1);
    }
    objLocal = m_env->CallStaticObjectMethod(clsParStream, midParStreamCon, m_env->NewStringUTF("STDIN"));
    check_and_treat_exception(m_env, "Error getting Stream.STDIN object");
    jobjParStreamSTDIN = (jobject)m_env->NewGlobalRef(objLocal);
    check_and_treat_exception(m_env, "Error getting Stream.STDIN object");

    objLocal = m_env->CallStaticObjectMethod(clsParStream, midParStreamCon, m_env->NewStringUTF("STDOUT"));
    check_and_treat_exception(m_env, "Error getting Stream.STDOUT object");
    jobjParStreamSTDOUT = (jobject)m_env->NewGlobalRef(objLocal);
    check_and_treat_exception(m_env, "Error getting Stream.STDOUT object");

    objLocal = m_env->CallStaticObjectMethod(clsParStream, midParStreamCon, m_env->NewStringUTF("STDERR"));
    check_and_treat_exception(m_env, "Error getting Stream.STDERR object");
    jobjParStreamSTDERR = (jobject)m_env->NewGlobalRef(objLocal);
    check_and_treat_exception(m_env, "Error getting Stream.STDERR object");

    objLocal = m_env->CallStaticObjectMethod(clsParStream, midParStreamCon, m_env->NewStringUTF("UNSPECIFIED"));
    check_and_treat_exception(m_env, "Error getting Stream.UNSPECIFIED object");
    jobjParStreamUNSPECIFIED = (jobject)m_env->NewGlobalRef(objLocal);
    check_and_treat_exception(m_env, "Error getting Stream.UNSPECIFIED object");


    // Parameter prefix empty
    jstring objStr = m_env->NewStringUTF("null");
    check_and_treat_exception(m_env, "Error getting null string object");
    jobjParPrefixEMPTY = (jstring)m_env->NewGlobalRef(objStr);
    check_and_treat_exception(m_env, "Error getting null string object");

    init_basic_jni_types();

    debug_printf ("[BINDING_COMMONS]  -  @Master JNI Init DONE\n");
}

int release_lock() {
    return pthread_mutex_unlock(&mtx);
}

int get_lock() {
    return pthread_mutex_lock(&mtx);
}

void process_param(void **params, int i, jobjectArray jobjOBJArr) {

    debug_printf("[BINDING_COMMONS]  -  @process_param\n");
    // params     is of the form: value type direction stream prefix name
    // jobjOBJArr is of the form: value type direction stream prefix name
    // This means that the ith parameters occupies the fields in the interval [NF * k, NK * k + 5]
    int pv = NUM_FIELDS * i + 0,
        pt = NUM_FIELDS * i + 1,
        pd = NUM_FIELDS * i + 2,
        ps = NUM_FIELDS * i + 3,
        pp = NUM_FIELDS * i + 4,
        pn = NUM_FIELDS * i + 5;

    void *parVal        =           params[pv];
    int parType         = *(int*)   params[pt];
    int parDirect       = *(int*)   params[pd];
    int parStream       = *(int*)   params[ps];
    char *parPrefix     = (char*)   params[pp];
    char *parName       = (char*)   params[pn];

    jclass clsParType = NULL; /* es.bsc.compss.types.annotations.parameter.DataType class */
    clsParType = m_env->FindClass("es/bsc/compss/types/annotations/parameter/DataType");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }

    jmethodID midParTypeCon = NULL; /* ID of the es.bsc.compss.api.COMPSsRuntime$DataType class constructor method */
    midParTypeCon = m_env->GetStaticMethodID(clsParType, "valueOf", "(Ljava/lang/String;)Les/bsc/compss/types/annotations/parameter/DataType;");
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }

    jobject jobjParType = NULL;
    jobject jobjParVal = NULL;

    debug_printf ("[BINDING-COMMONS]  -  @process_param  -  ENUM DATA_TYPE: %d\n", (enum datatype) parType);

    switch ( (enum datatype) parType) {
    case char_dt:
    case wchar_dt:
        jobjParVal = m_env->NewObject(clsCharacter, midCharCon, (jchar)*(char*)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Char: %c\n", *(char*)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("CHAR_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case boolean_dt:
        jobjParVal = m_env->NewObject(clsBoolean, midBoolCon, (jboolean)*(int*)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Bool: %d\n", *(int*)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("BOOLEAN_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case short_dt:
        jobjParVal = m_env->NewObject(clsShort, midShortCon, (jshort)*(short*)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Short: %hu\n", *(short*)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("SHORT_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case int_dt:
        jobjParVal = m_env->NewObject(clsInteger, midIntCon, (jint)*(int*)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Int: %d\n", *(int*)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("INT_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case long_dt:
        jobjParVal = m_env->NewObject(clsLong, midLongCon, (jlong)*(long*)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Long: %ld\n", *(long*)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("LONG_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case longlong_dt:
    case float_dt:
        jobjParVal = m_env->NewObject(clsFloat, midFloatCon, (jfloat)*(float*)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Float: %f\n", *(float*)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("FLOAT_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case double_dt:
        jobjParVal = m_env->NewObject(clsDouble, midDoubleCon, (jdouble)*(double*)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Double: %f\n", *(double*)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("DOUBLE_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case file_dt:
        jobjParVal = m_env->NewStringUTF(*(char **)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  File: %s\n", *(char **)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("FILE_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case external_psco_dt:
        jobjParVal = m_env->NewStringUTF(*(char **)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  Persistent: %s\n", *(char **)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("EXTERNAL_PSCO_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case string_dt:
        jobjParVal = m_env->NewStringUTF(*(char **)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  String: %s\n", *(char **)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("STRING_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
        break;
    case binding_object_dt:
        jobjParVal = m_env->NewStringUTF(*(char **)parVal);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }

        debug_printf ("[BINDING-COMMONS]  -  @process_param  -  BindingObject: %s\n", *(char **)parVal);

        jobjParType = m_env->CallStaticObjectMethod(clsParType, midParTypeCon, m_env->NewStringUTF("BINDING_OBJECT_T"));
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
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
    m_env->SetObjectArrayElement(jobjOBJArr, pv, jobjParVal);
    m_env->SetObjectArrayElement(jobjOBJArr, pt, jobjParType);

    // Add param direction
    debug_printf ("[BINDING-COMMONS]  -  @process_param  -  ENUM DIRECTION: %d\n", (enum direction) parDirect);
    switch ((enum direction) parDirect) {
    case in_dir:
        m_env->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirIN);
        break;
    case out_dir:
        m_env->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirOUT);
        break;
    case inout_dir:
        m_env->SetObjectArrayElement(jobjOBJArr, pd, jobjParDirINOUT);
        break;
    default:
        break;
    }

    // Add param stream
    debug_printf ("[BINDING-COMMONS]  -  @process_param  -  ENUM STREAM: %d\n", (enum stream) parStream);
    switch ((enum stream) parStream) {
    case STD_IN:
        m_env->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamSTDIN);
        break;
    case STD_OUT:
        m_env->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamSTDOUT);
        break;
    case STD_ERR:
        m_env->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamSTDERR);
        break;
    default:
        m_env->SetObjectArrayElement(jobjOBJArr, ps, jobjParStreamUNSPECIFIED);
        break;
    }

    // Add param prefix
    debug_printf ("[BINDING-COMMONS]  -  @process_param  -  PREFIX: %s\n", parPrefix);
    jstring jobjParPrefix = m_env->NewStringUTF(parPrefix);
    m_env->SetObjectArrayElement(jobjOBJArr, pp, jobjParPrefix);
    //env->SetObjectArrayElement(jobjOBJArr, pp, jobjParPrefixEMPTY);

    debug_printf ("[BINDING-COMMONS]  -  @process_param  -  NAME: %s\n", parName);
    jstring jobjParName = m_env->NewStringUTF(parName);
    m_env->SetObjectArrayElement(jobjOBJArr, pn, jobjParName);
}



// ******************************
// API functions
// ******************************

void GS_On(AbstractCache *absCache) {
    init_data_manager(absCache);
    GS_On();
}

void GS_On() {
    debug_printf ("[BINDING-COMMONS]  -  @GS_On\n");
    pthread_mutex_init(&mtx,NULL);
    clsITimpl = NULL;
    jmethodID midITImplConst = NULL;
    jmethodID midStartIT = NULL;

    init_env_vars();
    debug_printf ("[BINDING-COMMONS]  -  @GS_On  -  Creating the JVM\n");
    m_env = create_vm(&m_jvm);
    if (m_env == NULL) {
        printf ("[BINDING-COMMONS]  -  @GS_On  -  Error creating the JVM\n");
        exit(1);
    }
    // Obtaining Runtime Class
    jclass clsLocal = m_env->FindClass("es/bsc/compss/api/impl/COMPSsRuntimeImpl");
    check_and_treat_exception(m_env, "Error looking for the COMPSsRuntimeImpl class");
    clsITimpl = (jclass)m_env->NewGlobalRef(clsLocal);
    check_and_treat_exception(m_env, "Error looking for the COMPSsRuntimeImpl class");

    if (clsITimpl != NULL) {
        // Get constructor ID for COMPSsRuntimeImpl
        midITImplConst = m_env->GetMethodID(clsITimpl, "<init>", "()V");
        check_and_treat_exception(m_env, "Error looking for the init method");

        // Get startIT method ID
        midStartIT = m_env->GetMethodID(clsITimpl, "startIT", "()V");
        check_and_treat_exception(m_env,"Error looking for the startIT method");
    } else {
        printf("[BINDING-COMMONS]  -  @GS_On  -  Unable to find the runtime class\n");
        exit(1);
    }

    /************************************************************************/
    /* Now we will call the functions using the their method IDs            */
    /************************************************************************/

    if (midITImplConst != NULL) {
        if (clsITimpl != NULL && midITImplConst != NULL) {
            // Creating the Object of IT.
            debug_printf ("[BINDING-COMMONS]  -  @GS_On  -  Creating runtime object\n");
            jobject objLocal = m_env->NewObject(clsITimpl, midITImplConst);
            check_and_treat_exception(m_env,"Error creating runtime object");
            jobjIT = (jobject)m_env->NewGlobalRef(objLocal);
            check_and_treat_exception(m_env,"Error creating runtime object");
        } else {
            printf("[BINDING-COMMONS]  -  @GS_On  -  Unable to find the runtime constructor or runtime class\n");
            exit(1);
        }

        if (jobjIT != NULL && midStartIT != NULL) {
            debug_printf ("[BINDING-COMMONS]  -  @GS_On  -  Calling runtime start\n");
            m_env->CallVoidMethod(jobjIT, midStartIT); //Calling the method and passing IT Object as parameter
            check_and_treat_exception(m_env,"Error calling start runtime");
        } else {
            printf("[BINDING-COMMONS]  -  @GS_On  -  Unable to find the start method\n");
            exit(1);
        }
    } else {
        printf("[BINDING-COMMONS]  -  @GS_On  -  Unable to find the runtime constructor\n");
        exit(1);
    }

    init_master_jni_types();

    jobject objLocal = m_env->NewObject(clsLong, midLongCon, (jlong) 0);
    check_and_treat_exception(m_env,"Error creating appId object");
    appId = (jobject)m_env->NewGlobalRef(objLocal);
    check_and_treat_exception(m_env,"Error creating appId object");
}

void GS_Off() {
    debug_printf("[BINDING-COMMONS]  -  @GS_Off\n");

    jmethodID midStopIT = NULL;
    jmethodID midNoMoreTasksIT = NULL;

    check_and_attach(m_jvm, m_env);

    midNoMoreTasksIT = m_env->GetMethodID(clsITimpl, "noMoreTasks", "(Ljava/lang/Long;)V");
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @Off  -  Error: Exception loading noMoreTasks method.\n");
        m_env->ExceptionDescribe();
        exit(1);
    }

    midStopIT = m_env->GetMethodID(clsITimpl, "stopIT", "(Z)V");
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @Off  -  Error: Exception loading stop.\n");
        m_env->ExceptionDescribe();
        exit(1);
    }
    debug_printf("[BINDING-COMMONS]  -  @Off - Waiting to end tasks\n");
    m_env->CallVoidMethod(jobjIT, midNoMoreTasksIT, appId, "TRUE");
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @GS_ExecuteTask  -  Error: Exception received when calling noMoreTasks.\n");
        m_env->ExceptionDescribe();
        exit(1);
    }
    debug_printf("[BINDING-COMMONS]  -  @Off - Stopping runtime\n");
    m_env->CallVoidMethod(jobjIT, midStopIT, "TRUE"); //Calling the method and passing IT Object as parameter
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @Off  -  Error: Exception received when calling stop runtime.\n");
        m_env->ExceptionDescribe();
        exit(1);
    }
    debug_printf("[BINDING-COMMONS]  -  @Off - Removing JVM\n");
    destroy_vm(m_jvm);  // Release jvm resources -- Does not work properly --> JNI bug: not releasing properly the resources, so it is not possible to recreate de JVM.
    // delete jvm;    // free(): invalid pointer: 0x00007fbc11ba8020 ***
    m_jvm = NULL;
    debug_printf("[BINDING-COMMONS]  -  @Off - End\n");
    pthread_mutex_destroy(&mtx);
}

void GS_Get_AppDir(char **buf) {
    debug_printf ("[BINDING-COMMONS]  -  @GS_Get_AppDir - Getting application directory.\n");

    const char *cstr;
    jstring jstr = NULL;
    jboolean isCopy;

    get_lock();

    int isAttached = check_and_attach(m_jvm, m_env);

    jstr = (jstring)m_env->CallObjectMethod(jobjIT, midAppDir);
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @GS_Get_AppDir  -  Error: Exception received when calling getAppDir.\n");
        m_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }

    cstr = m_env->GetStringUTFChars(jstr, &isCopy);
    *buf = strdup(cstr);
    m_env->ReleaseStringUTFChars(jstr, cstr);
    if (isAttached == 1) {
        m_jvm->DetachCurrentThread();
    }
    release_lock();
    debug_printf("[BINDING-COMMONS]  -  @GS_Get_AppDir  -  directory name: %s\n", *buf);
}

void GS_ExecuteTask(long _appId, char *class_name, char *method_name, int priority, int has_target, int num_returns, int num_params, void **params) {

    jobjectArray jobjOBJArr; /* array of Objects to be passed to executeTask */

    debug_printf ("[BINDING-COMMONS]  -  @GS_ExecuteTask - Processing task execution in bindings-common. \n");
    //Default values
    bool _hasSignature = false;
    int num_nodes = 1;
    bool _isDistributed = false;
    bool _isReplicated = false;

    bool _priority = false;
    if (priority != 0) _priority = true;

    bool _has_target = false;
    if (has_target != 0) _has_target = true;

    get_lock();

    int isAttached = check_and_attach(m_jvm, m_env);
    
    jobject num_returns_integer = m_env->NewObject(clsInteger, midIntCon, num_returns);
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }

    jobjOBJArr = (jobjectArray)m_env->NewObjectArray(num_params * NUM_FIELDS, clsObject, m_env->NewObject(clsObject,midObjCon));

    for (int i = 0; i < num_params; i++) {
        debug_printf("[BINDING-COMMONS]  -  @GS_ExecuteTask  -  Processing parameter %d\n", i);
        process_param(params, i, jobjOBJArr);
    }

    m_env->CallVoidMethod(jobjIT, midExecute, appId, _hasSignature, m_env->NewStringUTF(class_name), m_env->NewStringUTF(method_name),  m_env->NewStringUTF(""), _priority, num_nodes, _isReplicated, _isDistributed, _has_target, num_returns_integer, num_params, jobjOBJArr);
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @GS_ExecuteTask  -  Error: Exception received when calling executeTask.\n");
        m_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }
    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    release_lock();
}

void GS_ExecuteTaskNew(long _appId, char *signature, int priority, int num_nodes, int replicated, int distributed,
                       int has_target, int num_returns, int num_params, void **params) {

    jobjectArray jobjOBJArr; /* array of Objects to be passed to executeTask */



    debug_printf ("[BINDING-COMMONS]  -  @GS_ExecuteTaskNew - Processing task execution in bindings-common. \n");

    bool _priority = false;
    if (priority != 0) _priority = true;

    bool _replicated = false;
    if (replicated != 0) _replicated = true;

    bool _distributed = false;
    if (distributed != 0) _distributed = true;

    bool _has_target = false;
    if (has_target != 0) _has_target = true;

    get_lock();

    int isAttached = check_and_attach(m_jvm, m_env);

    // Convert num_returns from int to integer
    jobject num_returns_integer = m_env->NewObject(clsInteger, midIntCon, num_returns);
    if (m_env->ExceptionOccurred()) {
        m_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }

    // Create array of parameters
    jobjOBJArr = (jobjectArray)m_env->NewObjectArray(num_params * NUM_FIELDS, clsObject, m_env->NewObject(clsObject,midObjCon));


    for (int i = 0; i < num_params; i++) {
        debug_printf("[BINDING-COMMONS]  -  @GS_ExecuteTaskNew  -  Processing parameter %d\n", i);
        process_param(params, i, jobjOBJArr);
    }

    // Call to JNI execute task method
    m_env->CallVoidMethod(jobjIT,
                          midExecuteNew,
                          appId,
                          m_env->NewStringUTF(signature),
                          _priority,
                          num_nodes,
                          _replicated,
                          _distributed,
                          _has_target,
                          num_returns_integer,
                          num_params,
                          jobjOBJArr);
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @GS_ExecuteTaskNew  -  Error: Exception received when calling executeTaskNew.\n");
        m_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }
    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    release_lock();
}

void GS_RegisterCE(char *CESignature, char *ImplSignature, char *ImplConstraints, char *ImplType, int num_params, char **ImplTypeArgs) {
    get_lock();
	int isAttached = check_and_attach(m_jvm, m_env);

    debug_printf ("[BINDING-COMMONS]  -  @GS_RegisterCE - Registering Core element.\n");
    //debug_printf ("[BINDING-COMMONS]  -  @GS_RegisterCE - CESignature:     %s\n", CESignature);
    //debug_printf ("[BINDING-COMMONS]  -  @GS_RegisterCE - ImplSignature:   %s\n", ImplSignature);
    //debug_printf ("[BINDING-COMMONS]  -  @GS_RegisterCE - ImplConstraints: %s\n", ImplConstraints);
    //debug_printf ("[BINDING-COMMONS]  -  @GS_RegisterCE - ImplType:        %s\n", ImplType);
    //debug_printf ("[BINDING-COMMONS]  -  @GS_RegisterCE - num_params:      %d\n", num_params);

    jobjectArray implArgs; //  array of Objects to be passed to register core element
    implArgs = (jobjectArray)m_env->NewObjectArray(num_params, clsString, m_env->NewStringUTF(""));
    for (int i = 0; i < num_params; i++) {
        //debug_printf("[BINDING-COMMONS]  -  @GS_RegisterCE  -    Processing pos %d\n", i);
        jstring tmp = m_env->NewStringUTF(ImplTypeArgs[i]);
        m_env->SetObjectArrayElement(implArgs, i, tmp);
    }
    m_env->CallVoidMethod(jobjIT, midRegisterCE, m_env->NewStringUTF(CESignature),
                          m_env->NewStringUTF(ImplSignature),
                          m_env->NewStringUTF(ImplConstraints),
                          m_env->NewStringUTF(ImplType),
                          implArgs);
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @GS_RegisterCE  -  Error: Exception received when calling registerCE.\n");
        m_env->ExceptionDescribe();
        release_lock();
        GS_Off();
        exit(1);
    }
    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    release_lock();
    debug_printf("[BINDING-COMMONS]  -  @GS_RegisterCE  -  Task registered: %s\n", CESignature);
}


void GS_Get_File(char *file_name, int mode, char **buf) {

    const char *cstr;
    jstring jstr = NULL;
    jboolean isCopy;

    get_lock();
    JNIEnv* local_env = m_env;
    int isAttached = check_and_attach(m_jvm, local_env);
    debug_printf("[BINDING-COMMONS]  -  @GS_Get_File  -  Calling runtime OpenFile method  for %s and mode %d ...\n", file_name, mode);
    jstring filename_str = local_env->NewStringUTF(file_name);
    check_and_treat_exception(local_env, "Error getting String UTF");
    release_lock();

    switch ((enum direction) mode) {
    case in_dir:
        jstr = (jstring)local_env->CallObjectMethod(jobjIT, midOpenFile, filename_str, jobjParDirIN);
        break;
    case out_dir:
        jstr = (jstring)local_env->CallObjectMethod(jobjIT, midOpenFile, filename_str, jobjParDirOUT);
        break;
    case inout_dir:
        jstr = (jstring)local_env->CallObjectMethod(jobjIT, midOpenFile, filename_str, jobjParDirINOUT);
        break;
    default:
        break;
    }
    check_and_treat_exception(local_env, "Error calling runtime openFile");
    local_env->DeleteLocalRef(filename_str);

    cstr = local_env->GetStringUTFChars(jstr, &isCopy);
    check_and_treat_exception(local_env, "Error getting String UTF");

    *buf = strdup(cstr);
    local_env->ReleaseStringUTFChars(jstr, cstr);
    local_env->DeleteLocalRef(jstr);

    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    debug_printf("[BINDING-COMMONS]  -  @GS_Get_File  -  COMPSs filename: %s\n", *buf);
}

void GS_Close_File(char *file_name, int mode) {
	get_lock();
    int isAttached = check_and_attach(m_jvm, m_env);
    debug_printf("[BINDING-COMMONS]  -  @GS_Close_File  -  Calling runtime closeFile method...\n");
    switch ((enum direction) mode) {
    case in_dir:
        m_env->CallVoidMethod(jobjIT, midCloseFile, m_env->NewStringUTF(file_name), jobjParDirIN);
        break;
    case out_dir:
        m_env->CallVoidMethod(jobjIT, midCloseFile, m_env->NewStringUTF(file_name), jobjParDirOUT);
        break;
    case inout_dir:
        m_env->CallVoidMethod(jobjIT, midCloseFile, m_env->NewStringUTF(file_name), jobjParDirINOUT);
        break;
    default:
        break;
    }
    check_and_treat_exception(m_env, "Error calling runtime closeFile");
    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    release_lock();
    debug_printf("[BINDING-COMMONS]  -  @GS_Close_File  -  COMPSs filename: %s\n", file_name);
}

void GS_Delete_File(char *file_name) {

	get_lock();
    int isAttached = check_and_attach(m_jvm, m_env);

    jboolean res = m_env->CallBooleanMethod(jobjIT, midDeleteFile, m_env->NewStringUTF(file_name));
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @GS_Delete_File  -  Error: Exception received when calling deleteFile.\n");
        m_env->ExceptionDescribe();
        release_lock();
        GS_Off();
        exit(1);
    }
    //*buf = (int*)&res;
    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    release_lock();
    debug_printf("[BINDING-COMMONS]  -  @GS_Delete_File  -  COMPSs filename: %s\n", file_name);
}

void GS_Get_Object(char *file_name, char**buf) {

	const char *cstr;
    jstring jstr = NULL;
    jboolean isCopy;
    get_lock();
    JNIEnv* local_env = m_env;
    int isAttached = check_and_attach(m_jvm, local_env);
    release_lock();
    jstr = (jstring)local_env->CallObjectMethod(jobjIT, midgetBindingObject, local_env->NewStringUTF(file_name));

    if (local_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @GS_Get_Object  -  Error: Exception received when calling getObject.\n");
        local_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }

    cstr = local_env->GetStringUTFChars(jstr, &isCopy);
    *buf = strdup(cstr);
    local_env->ReleaseStringUTFChars(jstr, cstr);
    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    debug_printf("[BINDING-COMMONS]  -  @GS_Get_Object  -  COMPSs data id: %s\n", *buf);

}

void GS_Delete_Object(char *file_name, int **buf) {
	get_lock();

	int isAttached = check_and_attach(m_jvm, m_env);

    jboolean res = m_env->CallBooleanMethod(jobjIT, midDeleteBindingObject, m_env->NewStringUTF(file_name));
    if (m_env->ExceptionOccurred()) {
        debug_printf("[BINDING-COMMONS]  -  @GS_Delete_Binding_Object  -  Error: Exception received when calling deleteObject.\n");
        m_env->ExceptionDescribe();
        release_lock();
        exit(1);
    }
    *buf = (int*)&res;
    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    release_lock();
    debug_printf("[BINDING-COMMONS]  -  @GS_Delete_Binding_Object  -  COMPSs obj: %s\n", file_name);
}

void GS_Barrier(long _appId) {
	debug_printf("[BINDING-COMMONS]  -  @GS_Barrier  -  Waiting tasks for APP id: %lu\n", appId);
    get_lock();
    JNIEnv* local_env = m_env;
    int isAttached = check_and_attach(m_jvm, local_env);
	release_lock();

	local_env->CallVoidMethod(jobjIT, midBarrier, appId);

    if (local_env->ExceptionOccurred()) {
        local_env->ExceptionDescribe();
        exit(1);
    }

    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    debug_printf("[BINDING-COMMONS]  -  @GS_Barrier  -  APP id: %lu\n", appId);
}

void GS_BarrierNew(long _appId, int noMoreTasks) {
	get_lock();
	JNIEnv* local_env = m_env;
	int isAttached = check_and_attach(m_jvm, local_env);

    bool _noMoreTasks = false;
    if (noMoreTasks != 0) _noMoreTasks = true;

    debug_printf("[   BINDING]  -  @GS_Barrier  -  Waiting tasks for APP id: %lu\n", appId);
    debug_printf("[   BINDING]  -  @GS_Barrier  -  noMoreTasks: %s\n", _noMoreTasks ? "true":"false");
    release_lock();

    local_env->CallVoidMethod(jobjIT, midBarrierNew, appId, _noMoreTasks);

    if (local_env->ExceptionOccurred()) {
        local_env->ExceptionDescribe();

        exit(1);
    }

    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    debug_printf("[BINDING-COMMONS]  -  @GS_Barrier  -  APP id: %lu\n", appId);
}

void GS_EmitEvent(int type, long id) {
	get_lock();
	int isAttached = check_and_attach(m_jvm, m_env);

    if ( (type < 0 ) or (id < 0) ) {
        debug_printf ("[BINDING-COMMONS]  -  @GS_EmitEvent  -  Error: event type and ID must be positive integers, but found: type: %u, ID: %lu\n", type, id);
        exit(1);
    } else {
        debug_printf ("[BINDING-COMMONS]  -  @GS_EmitEvent  -  Type: %u, ID: %lu\n", type, id);
        m_env->CallVoidMethod(jobjIT, midEmitEvent, type, id);
        if (m_env->ExceptionOccurred()) {
            m_env->ExceptionDescribe();
            release_lock();
            exit(1);
        }
    }

    if (isAttached==1) {
        m_jvm->DetachCurrentThread();
    }
    release_lock();
}
