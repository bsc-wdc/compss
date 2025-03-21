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
#include "AbstractCache.h"
/* Header for class es_bsc_compss_data_BindingDataManager */

#ifndef BindingDataManager
#define BindingDataManager

void init_data_manager(AbstractCache *newcache);

AbstractCache *get_cache();

void addGlobalRef(void* pointer, jobject jobj);

void removeGlobalRef(JNIEnv *env, void* pointer);

int hasGlobalRef(void* pointer);

void cleanGlobalRefs(JNIEnv *env);

void *sync_object_from_runtime(long app_id, char* name, int type, int elements);

int delete_object_from_runtime(char* name, int type, int elements);

#ifdef __cplusplus
extern "C" {
#endif/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    isInBinding
 * Signature: (Ljava/lang/String;)Z
 */
JNIEXPORT jboolean JNICALL Java_es_bsc_compss_data_BindingDataManager_isInBinding
(JNIEnv *, jclass, jstring);

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    removeData
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_removeData
(JNIEnv *, jclass, jstring);

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    storeInFile
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_storeInFile
(JNIEnv *, jclass, jstring, jstring);

JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_copyCachedData
(JNIEnv *, jclass, jstring, jstring);

JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_moveCachedData
(JNIEnv *, jclass, jstring, jstring);
/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    loadFromFile
 * Signature: (Ljava/lang/String;Ljava/lang/String;II)I
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_loadFromFile
(JNIEnv *, jclass, jstring, jstring, jint, jint);

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    getByteArray
 * Signature: (Ljava/lang/String;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_es_bsc_compss_data_BindingDataManager_getByteArray
(JNIEnv *, jclass, jstring);

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    setByteArray
 * Signature: (Ljava/lang/String;Ljava/nio/ByteBuffer;II)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_setByteArray
(JNIEnv *, jclass, jstring, jobject, jint, jint);

/*
 * Class:     es_bsc_compss_nio_utils_NIOBindingDataManager
 * Method:    sendNativeObject
 * Signature: (Ljava/lang/String;Les/bsc/compss/nio/utils/NIOBindingObjectStream;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_nio_utils_NIOBindingDataManager_sendNativeObject
(JNIEnv *, jclass, jstring, jobject);

/*
 * Class:     es_bsc_compss_nio_utils_NIOBindingDataManager
 * Method:    receiveNativeObject
 * Signature: (Ljava/lang/String;Les/bsc/compss/nio/utils/NIOBindingObjectStream;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_nio_utils_NIOBindingDataManager_receiveNativeObject
(JNIEnv *, jclass, jstring, jint type, jobject);

#ifdef __cplusplus
}
#endif
#endif
