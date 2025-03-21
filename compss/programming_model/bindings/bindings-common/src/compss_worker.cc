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

#include "common.h"
#include "common_jni.h"
#include "compss_worker.h"
#include "BindingDataManager.h"
#include "BindingExecutor.h"


#define NUM_PARAMS 5
using namespace std;

JNIEnv *w_env;
jclass clsNioWorker;
JavaVM * w_jvm;
jclass w_clsString;       /*  java.lang.String class */
jmethodID w_midStrCon;    	/* ID of the java.lang.String class constructor method */

void init_worker_jni_types() {

    // Parameter classes
    debug_printf ("[BINDING-COMMONS]  -  @Init JNI Types\n");

    w_clsString = w_env->FindClass("java/lang/String");
    if (w_env->ExceptionOccurred()) {
        w_env->ExceptionDescribe();
        exit(1);
    }
    w_midStrCon = w_env->GetMethodID(w_clsString, "<init>", "(Ljava/lang/String;)V");
    if (w_env->ExceptionOccurred()) {
        w_env->ExceptionDescribe();
        exit(1);
    }
}

jobjectArray convertToJavaArgs(int argc, char** args) {
    int i;
    jobjectArray args_java = w_env->NewObjectArray( argc, w_clsString, NULL);
    for (i=0; i<argc; i++) {
        jstring arg= w_env->NewStringUTF(args[i]);
        w_env->SetObjectArrayElement(args_java, i, arg);
    }
    return args_java;
}


void worker_start(AbstractCache *absCache, AbstractExecutor *absExecutor, int argc, char** args) {
    jclass clsNioWorker = NULL;
    jmethodID midNioWorkerMain = NULL;

    init_env_vars();

    init_data_manager(absCache);

    init_executor(absExecutor);

    w_env = create_vm(&w_jvm);
    if (w_env == NULL) {
        print_error ("[BINDING-COMMONS]  -  @GS_On  -  Error creating the JVM\n");
        exit(1);
    }

    init_worker_jni_types();

    //Obtaining Classes
    clsNioWorker = w_env->FindClass("es/bsc/compss/nio/worker/NIOWorker");
    if (w_env->ExceptionOccurred()) {
        w_env->ExceptionDescribe();
        print_error("[BINDING-COMMONS]  -  @GS_On  -  Error looking for the COMPSsRuntimeImpl class\n");
        exit(1);
    }

    if (clsNioWorker != NULL) {
        //Get constructor ID for COMPSsRuntimeImpl

        midNioWorkerMain = w_env->GetStaticMethodID(clsNioWorker, "main", "([Ljava/lang/String;)V");
        if (w_env->ExceptionOccurred()) {
            w_env->ExceptionDescribe();
            print_error("[BINDING-COMMONS]  -  @GS_On  -  Error looking for the init method\n");
            exit(1);
        }
        jobjectArray args_java = convertToJavaArgs(argc, args);
        debug_printf ("[BINDING-COMMONS]  -  @Starting NIO Worker\n");
        if (midNioWorkerMain != NULL ) {
            w_env->CallStaticVoidMethod(clsNioWorker, midNioWorkerMain, args_java); //Calling the method and passing IT Object as parameter
            if (w_env->ExceptionOccurred()) {
                w_env->ExceptionDescribe();
                print_error("[BINDING-COMMONS]  -  @GS_On  - Error calling worker main\n");
                exit(1);
            }
        }
        debug_printf ("[BINDING-COMMONS]  -  @Worker ended\n");
    }
}
