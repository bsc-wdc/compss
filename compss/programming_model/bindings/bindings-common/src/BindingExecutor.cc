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
#include "common.h"
#include "BindingExecutor.h"


AbstractExecutor *executor;

void init_executor(AbstractExecutor *aExecutor) {
    debug_printf("Initializing executor with %p", aExecutor);
    executor=aExecutor;
}

/*
 * Class:     es_bsc_compss_nio_worker_executors_PersistentExternalExecutor
 * Method:    executeInBinding
 * Signature: (I[Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_es_bsc_compss_invokers_external_persistent_PersistentInvoker_executeInBinding
//(JNIEnv *env, jclass jClass, jint argc_java, jobjectArray args_java){
(JNIEnv *env, jclass jClass, jstring args_java) {
    char *result;
    /*int size = (jint)argc_java;
    jstring str[size];
    char **args;//char args = (char**)malloc(sizeof(char*)*argc_java);
    for(int i=0; i < size; i++){
        str[i] = (jstring)env->GetObjectArrayElement(args_java, i);
    	args[i] = env->GetStringUTFChars(str[i], 0);
    }
    executor->executeTask(size, args, result);
    for(int i=0; i < size; i++){
        env->ReleaseStringUTFChars(str[i], args[i]);
    }*/
    const char * args = env->GetStringUTFChars(args_java, 0);

    debug_printf("[Binding-Executor] Receiving command at binding to execute a task : \n \tCOMMAND: %s", args);
    executor->executeTask(args, result);
    env->ReleaseStringUTFChars(args_java, args);
    debug_printf("[Binding-Executor] Result is : %s", result);
    jstring result_java = env->NewStringUTF(result);
    return result_java;
}

/*
 * Class:     es_bsc_compss_nio_worker_executors_PersistentExternalExecutor
 * Method:    finishThread
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_es_bsc_compss_invokers_external_persistent_PersistentInvoker_finishThread
(JNIEnv *env, jclass jClass) {
    debug_printf("Receiving command to finish a worker thread");
    executor->finishThread();
}

/*
 * Class:     es_bsc_compss_nio_worker_executors_PersistentExternalExecutor
 * Method:    initThread
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_es_bsc_compss_invokers_external_persistent_PersistentInvoker_initThread
(JNIEnv *env, jclass jClass) {
    debug_printf("Receiving command to initialize a worker thread");
    executor->initThread();
}

