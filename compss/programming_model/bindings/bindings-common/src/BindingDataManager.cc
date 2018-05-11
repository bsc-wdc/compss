#include <iostream>
#include <sstream>
#include <string.h>
#include "BindingDataManager.h"
#include "GS_compss.h"
#include "common.h"
# define BUFFER 65536

AbstractCache *cache = NULL;

void init_data_manager(AbstractCache *newcache){
	debug_printf("Initializing data manager with cache %p", newcache);
	cache = newcache;
}

AbstractCache *get_cache(){
	return cache;
}

int delete_object_from_runtime(char* name, int type, int elements) {
	stringstream ss;
	ss << name << "#" << type << "#" << elements << flush;
	char * binding_obj_id = strdup(ss.str().c_str());
	//Deleting object from runtime
	int *res;
	GS_Delete_Object(binding_obj_id, &res);
	//Check if original name is in bindings cache. Remove if it is cache
	AbstractCache *cache = get_cache();
	if (cache->isInCache(name)){
		cache->deleteFromCache(name, false);
	}
}

void *sync_object_from_runtime(char* name, int type, int elements) {
	char *runtime_filename;
	stringstream ss;
	ss << name << "#" << type << "#" << elements << flush;
	char * binding_obj_id = strdup(ss.str().c_str());
	debug_printf("[BindingDataManager]  -  Calling runtime to get binding object %s to binding cache.\n",	binding_obj_id);
	GS_Get_Object(binding_obj_id, &runtime_filename);
	compss_pointer cp;
	AbstractCache *cache = get_cache();
	debug_printf("[BindingDataManager]  -  Getting object %s from cache.\n",	runtime_filename);
	cache->getFromCache(runtime_filename, cp);
	debug_printf("[BindingDataManager]  -  Deleting object %s from cache.\n",	runtime_filename);
	cache->deleteFromCache(runtime_filename, false);
	debug_printf("[BindingDataManager]  -  Calling runtime to delete binding object %s.\n", binding_obj_id);
	int *res;
	GS_Delete_Object(binding_obj_id, &res);
	return cp.pointer;
}
/*
 * Class:     es_bsc_compss_util_BindingDataManager
 * Method:    isInBinding
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT jboolean JNICALL Java_es_bsc_compss_util_BindingDataManager_isInBinding(JNIEnv *env, jclass jClass, jstring id){
	const char *id_str = env->GetStringUTFChars(id, 0);
	jboolean res = (jboolean)cache->isInCache(id_str);
	env->ReleaseStringUTFChars( id, id_str);
	return res;
}



/*
 * Class:     es_bsc_compss_util_BindingDataManager
 * Method:    removeData
 * Signature: (Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_util_BindingDataManager_removeData(JNIEnv *env, jclass jClass, jstring id){
	const char *id_str = env->GetStringUTFChars(id, 0);
	jint res = (jint)AbstractCache::removeData(id_str, *cache);
	env->ReleaseStringUTFChars( id, id_str);
	return res;
}

/*
 * Class:     es_bsc_compss_util_BindingDataManager
 * Method:    storeInFile
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_util_BindingDataManager_storeInFile(JNIEnv *env, jclass jClass, jstring id, jstring filename){
	const char *id_str = env->GetStringUTFChars(id, 0);
	const char *filename_str = env->GetStringUTFChars(filename, 0);
	jint res = (jint)cache->pushToFile(id_str, filename_str);
	env->ReleaseStringUTFChars( id, id_str);
	env->ReleaseStringUTFChars( filename, filename_str);
	return res;
}

/*
 * Class:     es_bsc_compss_util_BindingDataManager
 * Method:    loadFromFile
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_util_BindingDataManager_loadFromFile(JNIEnv *env, jclass jClass, jstring id, jstring filename, jint type, jint elements){
	const char *id_str = env->GetStringUTFChars(id, 0);
	const char *filename_str = env->GetStringUTFChars(filename, 0);
	compss_pointer cp;
	cp.type = (int)type;
	cp.elements = (int)elements;
	jint res = (jint)cache->pullFromFile(id_str, filename_str, cp);
	env->ReleaseStringUTFChars( id, id_str);
	env->ReleaseStringUTFChars( filename, filename_str);
	return res;
}

/*
 * Class:     es_bsc_compss_util_BindingDataManager
 * Method:    copyCachedData
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_util_BindingDataManager_copyCachedData(JNIEnv *env, jclass jClass, jstring from_id, jstring to_id){
	const char *from_id_str = env->GetStringUTFChars(from_id, 0);
	const char *to_id_str = env->GetStringUTFChars(to_id, 0);
	compss_pointer cp;
	jint res = (jint)cache->copyInCache(from_id_str, to_id_str, cp);
	env->ReleaseStringUTFChars( from_id, from_id_str);
	env->ReleaseStringUTFChars( to_id, to_id_str);
	return res;
}

/*
 * Class:     es_bsc_compss_util_BindingDataManager
 * Method:    copyCachedData
 * Signature: (Ljava/lang/String;Ljava/lang/String;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_util_BindingDataManager_moveCachedData(JNIEnv *env, jclass jClass, jstring from_id, jstring to_id){
	const char *from_id_str = env->GetStringUTFChars(from_id, 0);
	const char *to_id_str = env->GetStringUTFChars(to_id, 0);
	jint res = (jint)cache->moveInCache(from_id_str, to_id_str);
	env->ReleaseStringUTFChars( from_id, from_id_str);
	env->ReleaseStringUTFChars( to_id, to_id_str);
	return res;
}

/*
 * Class:     es_bsc_compss_util_BindingDataManager
 * Method:    getByteArray
 * Signature: (Ljava/lang/String;)Ljava/nio/ByteBuffer;
 */
JNIEXPORT jobject JNICALL Java_es_bsc_compss_util_BindingDataManager_getByteArray
  (JNIEnv *env, jclass jClass, jstring id){
	jobject o = NULL;
	const char *id_str = env->GetStringUTFChars(id, 0);
	compss_pointer cp;
	int res = cache->getFromCache(id_str, cp);
	if (res == 0){
		void* p = cp.pointer;
		int size = cp.size;
		debug_printf("[BindingDataManager] Getting from cache Buff: %p size: %d\n", p, size);
		o = env->NewDirectByteBuffer(p,size);
	}else{
		cout << "[BindingDataManager] Error getting data " << id_str << endl;
	}
	env->ReleaseStringUTFChars( id, id_str);
	return o;
}

/*
 * Class:     es_bsc_compss_util_BindingDataManager
 * Method:    setByteArray
 * Signature: (Ljava/lang/String;Ljava/nio/ByteBuffer;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_util_BindingDataManager_setByteArray(JNIEnv *env, jclass jClass, jstring id, jobject jobj, jint type, jint elements){
	const char *id_str = env->GetStringUTFChars(id, 0);
	compss_pointer cp;
	cp.pointer = env->GetDirectBufferAddress(jobj);
	cp.size = (long)env->GetDirectBufferCapacity(jobj);
	cp.type = (int)type;
	cp.elements = (int)elements;
	//printf("Storing in cache Buff: %d size: %d\n", cp.pointer, cp.size);
	jint res = (jint)cache->storeInCache(id_str, cp);
	env->ReleaseStringUTFChars( id, id_str);
	return res;
}

/*
 * Class:     es_bsc_compss_nio_utils_NIOBindingDataManager
 * Method:    sendNativeObject
 * Signature: (Ljava/lang/String;Les/bsc/compss/nio/utils/NIOBindingObjectStream;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_nio_utils_NIOBindingDataManager_sendNativeObject(JNIEnv *env, jclass jOTClass, jstring id, jobject nio_stream){
	const char *id_str = env->GetStringUTFChars(id, 0);
	JavaNioConnStreamBuffer jsb(env, nio_stream, BUFFER);
	jint res = (jint)cache->pushToStream(id_str, jsb);
	env->ReleaseStringUTFChars( id, id_str);
	return res;
}
/*
 * Class:     es_bsc_compss_nio_utils_NIOBindingDataManager
 * Method:    receiveNativeObject
 * Signature: (Ljava/lang/String;Les/bsc/compss/nio/utils/NIOBindingObjectStream;)V
 */
JNIEXPORT jint JNICALL Java_es_bsc_compss_nio_utils_NIOBindingDataManager_receiveNativeObject
  (JNIEnv *env, jclass jClass, jstring id, jint type, jobject nio_stream){
    const char *id_str = env->GetStringUTFChars(id, 0);
	JavaNioConnStreamBuffer jsb(env, nio_stream, BUFFER);
	compss_pointer cp;
	cp.type = (int)type;
	cp.elements = 0;
	jint res = (jint)cache->pullFromStream(id_str, jsb, cp);
	env->ReleaseStringUTFChars( id, id_str);
	return res;
}

