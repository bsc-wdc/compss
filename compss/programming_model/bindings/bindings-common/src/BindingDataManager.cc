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
#include <iostream>
#include <sstream>
#include <string.h>
#include "BindingDataManager.h"
#include "GS_compss.h"
#include "common.h"
# define BUFFER 65536

AbstractCache *cache = NULL;
//map to store globalRefs
map<void*, jobject> globalRefs;

void init_data_manager(AbstractCache *newcache) {
    debug_printf("Initializing data manager with cache %p", newcache);
    cache = newcache;
}

AbstractCache *get_cache() {
    return cache;
}

void addGlobalRef(void* pointer, jobject jobj){
	globalRefs[pointer] = jobj;
}

void removeGlobalRef(JNIEnv *env, void* pointer){
	if (pointer!=NULL){
		if (hasGlobalRef(pointer)){
			jobject jobj = globalRefs[pointer];
			env->DeleteGlobalRef(jobj);
		}
	}
}

int hasGlobalRef(void* pointer){
	return (globalRefs.count(pointer) > 0);
}

void cleanGlobalRefs(JNIEnv *env){
	for(std::map<void*, jobject>::iterator it = globalRefs.begin(); it != globalRefs.end(); it++) {
	     env->DeleteGlobalRef(it->second);
	}
}

int delete_object_from_runtime(long app_id, char* name, int type, int elements) {
    stringstream ss;
    ss << name << "#" << type << "#" << elements << flush;
    char * binding_obj_id = strdup(ss.str().c_str());
    //Deleting object from runtime
    int *res;
    GS_Delete_Object(app_id, binding_obj_id, &res);
    //Check if original name is in bindings cache. Remove if it is cache
    AbstractCache *cache = get_cache();
    if (cache == NULL) {
        if (cache->isInCache(name)) {
            cache->deleteFromCache(name, false);
        }
    }
    return 0;
}

void *sync_object_from_runtime(long app_id, char* name, int type, int elements) {
    char *runtime_filename;
    stringstream ss;
    ss << name << "#" << type << "#" << elements << flush;
    char * binding_obj_id = strdup(ss.str().c_str());
    debug_printf("[BindingDataManager]  -  Calling runtime to get binding object %s to binding cache.\n",	binding_obj_id);
    GS_Get_Object(app_id, binding_obj_id, &runtime_filename);
    compss_pointer cp;
    AbstractCache *cache = get_cache();
    if (cache != NULL) {
        debug_printf("[BindingDataManager]  -  Getting object %s from cache.\n",	runtime_filename);
        int get_res = cache->getFromCache(runtime_filename, cp);
        if (get_res != 0) {
            print_error("[BindingDataManager] - ERROR - Getting object %s from cache.\n", runtime_filename);
            GS_Off(get_res);
            exit(get_res);
        }
        debug_printf("[BindingDataManager]  -  Object %s has ref. %p\n",  runtime_filename, cp.pointer);
        debug_printf("[BindingDataManager]  -  Deleting object %s from cache.\n",	runtime_filename);
        cache->deleteFromCache(runtime_filename, false);
        debug_printf("[BindingDataManager]  -  Calling runtime to delete binding object %s.\n", binding_obj_id);
        int *res;
        GS_Delete_Object(app_id, binding_obj_id, &res);
        return cp.pointer;
    } else {
        print_error("[BindingDataManager] - ERROR - Cache is null when synchronizing object.\n");
        GS_Off(1);
        exit(1);
    }
}
/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    isInBinding
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT jboolean JNICALL Java_es_bsc_compss_data_BindingDataManager_isInBinding(JNIEnv *env, jclass jClass, jstring id) {
    if (cache != NULL) {
        const char *id_str = env->GetStringUTFChars(id, 0);
        jboolean res = (jboolean)cache->isInCache(id_str);
        env->ReleaseStringUTFChars( id, id_str);
        return res;
    } else {
        debug_printf("[BindingDataManager]  - cache is null when checking if data is in binding. Returning false;\n");
        return (jboolean)false;
    }
}



/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    removeData
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_removeData(JNIEnv *env, jclass jClass, jstring id) {
    if (cache != NULL) {
        const char *id_str = env->GetStringUTFChars(id, 0);
        compss_pointer cp;
        jint res = (jint)cache->getFromCache(id_str, cp);
        if (res == 0) {
        	jint res2 = (jint)AbstractCache::removeData(id_str, *cache);
        	env->ReleaseStringUTFChars( id, id_str);
        	removeGlobalRef(env,cp.pointer);
        	return res2;
        }else{
        	debug_printf("[BindingDataManager]  - Object not in cache ignoring remove.\n");
        	cache->printValues();
        }
    } else {
        debug_printf("[BindingDataManager]  - Error: cache is null when removing data.\n");
        return (jint)-1;
    }

    return (jint)0;
}

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    storeInFile
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_storeInFile(JNIEnv *env, jclass jClass, jstring id, jstring filename) {
    if (cache != NULL) {
        const char *id_str = env->GetStringUTFChars(id, 0);
        const char *filename_str = env->GetStringUTFChars(filename, 0);
        jint res = (jint)cache->pushToFile(id_str, filename_str);
        env->ReleaseStringUTFChars( id, id_str);
        env->ReleaseStringUTFChars( filename, filename_str);
        return res;
    } else {
        debug_printf("[BindingDataManager]  - Error: cache is null when storing in file.\n");
        return (jint)-1;
    }
}

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    loadFromFile
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_loadFromFile(JNIEnv *env, jclass jClass, jstring id, jstring filename, jint type, jint elements) {
    if (cache != NULL) {
        const char *id_str = env->GetStringUTFChars(id, 0);
        const char *filename_str = env->GetStringUTFChars(filename, 0);
        compss_pointer cp;
        cp.type = (int)type;
        cp.elements = (int)elements;
        jint res = (jint)cache->pullFromFile(id_str, filename_str, cp);
        env->ReleaseStringUTFChars( id, id_str);
        env->ReleaseStringUTFChars( filename, filename_str);
        return res;
    } else {
        print_error("[BindingDataManager]  - Error: cache is null when loading from file.\n");
        return (jint)-1;
    }
}

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    copyCachedData
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_copyCachedData(JNIEnv *env, jclass jClass, jstring from_id, jstring to_id) {
    if (cache != NULL) {
        const char *from_id_str = env->GetStringUTFChars(from_id, 0);
        const char *to_id_str = env->GetStringUTFChars(to_id, 0);
        compss_pointer cp;
        jint res = (jint)cache->copyInCache(from_id_str, to_id_str, cp);
        env->ReleaseStringUTFChars( from_id, from_id_str);
        env->ReleaseStringUTFChars( to_id, to_id_str);
        return res;
    } else {
        print_error("[BindingDataManager]  - Error: cache is null when copying cached data.\n");
        return (jint)-1;
    }
}

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    copyCachedData
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_moveCachedData(JNIEnv *env, jclass jClass, jstring from_id, jstring to_id) {
    if (cache != NULL) {
        const char *from_id_str = env->GetStringUTFChars(from_id, 0);
        const char *to_id_str = env->GetStringUTFChars(to_id, 0);
        jint res = (jint)cache->moveInCache(from_id_str, to_id_str);
        env->ReleaseStringUTFChars( from_id, from_id_str);
        env->ReleaseStringUTFChars( to_id, to_id_str);
        return res;
    } else {
        print_error("[BindingDataManager]  - Error: cache is null when moving cached data.\n");
        return (jint)-1;
    }
}

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    getByteArray
 * Signature: (Ljava/lang/String;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_es_bsc_compss_data_BindingDataManager_getByteArray
(JNIEnv *env, jclass jClass, jstring id) {
    jobject o = NULL;
    if (cache != NULL) {
        const char *id_str = env->GetStringUTFChars(id, 0);
        compss_pointer cp;
        int res = cache->getFromCache(id_str, cp);
        if (res == 0) {
            void* p = cp.pointer;
            int size = cp.size;
            debug_printf("[BindingDataManager] Getting from cache Buff: %p size: %d\n", p, size);
            o = env->NewDirectByteBuffer(p,size);
        } else {
            cout << "[BindingDataManager] Error getting data " << id_str << endl;
        }
        env->ReleaseStringUTFChars( id, id_str);
    } else {
        print_error("[BindingDataManager]  - Error: cache is null when getting bytearray.\n");
    }
    return o;
}

/*
 * Class:     es_bsc_compss_data_BindingDataManager
 * Method:    setByteArray
 * Signature: (Ljava/lang/String;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_data_BindingDataManager_setByteArray(JNIEnv *env, jclass jClass, jstring id, jobject jobj, jint type, jint elements) {
	if (cache != NULL) {
        const char *id_str = env->GetStringUTFChars(id, 0);
        compss_pointer cp;
        jobject jobjGlobal = env->NewGlobalRef(jobj);
        cp.pointer = env->GetDirectBufferAddress(jobjGlobal);
        addGlobalRef(cp.pointer, jobjGlobal);
        cp.size = (long)env->GetDirectBufferCapacity(jobj);
        cp.type = (int)type;
        cp.elements = (int)elements;
        debug_printf("[BindingDataManager] Storing %s in cache Buff: %p size: %ld\n", id_str, cp.pointer, cp.size);
        jint res = (jint)cache->storeInCache(id_str, cp);
        env->ReleaseStringUTFChars( id, id_str);
        return res;
    } else {
        print_error("[BindingDataManager]  - Error: cache is null when setting bytearray.\n");
        return (jint)-1;
    }
}

/*
 * Class:     es_bsc_compss_nio_utils_NIOBindingDataManager
 * Method:    sendNativeObject
 * Signature: (Ljava/lang/String;Les/bsc/compss/nio/utils/NIOBindingObjectStream;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_nio_utils_NIOBindingDataManager_sendNativeObject(JNIEnv *env, jclass jOTClass, jstring id, jobject nio_stream) {
    if (cache != NULL) {
        const char *id_str = env->GetStringUTFChars(id, 0);
        JavaNioConnStreamBuffer jsb(env, nio_stream, BUFFER);
        jint res = (jint)cache->pushToStream(id_str, jsb);
        env->ReleaseStringUTFChars( id, id_str);
        return res;
    } else {
        print_error("[BindingDataManager]  - Error: cache is null when sending native object.\n");
        return (jint)-1;
    }
}
/*
 * Class:     es_bsc_compss_nio_utils_NIOBindingDataManager
 * Method:    receiveNativeObject
 * Signature: (Ljava/lang/String;Les/bsc/compss/nio/utils/NIOBindingObjectStream;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_nio_utils_NIOBindingDataManager_receiveNativeObject
(JNIEnv *env, jclass jClass, jstring id, jint type, jobject nio_stream) {
    if (cache != NULL) {
        const char *id_str = env->GetStringUTFChars(id, 0);
        JavaNioConnStreamBuffer jsb(env, nio_stream, BUFFER);
        compss_pointer cp;
        cp.type = (int)type;
        cp.elements = 0;
        jint res = (jint)cache->pullFromStream(id_str, jsb, cp);

        env->ReleaseStringUTFChars( id, id_str);
        return res;
    } else {
        print_error("[BindingDataManager]  - Error: cache is null when receiving native object.\n");
        return (jint)-1;
    }
}

