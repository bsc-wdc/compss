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
#include <jni.h>
#include "AbstractExecutor.h"
/* Header for class es_bsc_compss_nio_worker_executors_PersistentExternalExecutor */

#ifndef _BindingExecutor
#define _BindingExecutor

void init_executor(AbstractExecutor *aExecutor);

#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     es_bsc_compss_nio_worker_executors_PersistentExternalExecutor
 * Method:    executeInBinding
 * Signature: (I[Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_es_bsc_compss_invokers_external_persistent_PersistentInvoker_executeInBinding
(JNIEnv *, jclass, jstring);
// (JNIEnv *, jclass, jint, jobjectArray);

/*
 * Class:     es_bsc_compss_nio_worker_executors_PersistentExternalExecutor
 * Method:    finishThread
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_es_bsc_compss_invokers_external_persistent_PersistentInvoker_finishThread
(JNIEnv *, jclass);

/*
 * Class:     es_bsc_compss_nio_worker_executors_PersistentExternalExecutor
 * Method:    initThread
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_es_bsc_compss_invokers_external_persistent_PersistentInvoker_initThread
(JNIEnv *, jclass);
#ifdef __cplusplus
}
#endif
#endif
