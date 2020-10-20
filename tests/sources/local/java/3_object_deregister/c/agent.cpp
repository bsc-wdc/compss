#include <jvmti.h>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>

using namespace std;

typedef struct {
  jvmtiEnv *jvmti;
} GlobalAgentData;

static GlobalAgentData *gdata;

JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM *jvm, char *options, void *reserved)
{
  jvmtiEnv *jvmti = NULL;
  jvmtiCapabilities capa;
  jvmtiError error;

  jint result = jvm->GetEnv((void **) &jvmti, JVMTI_VERSION_1_1);
  if (result != JNI_OK) {
 	  printf("ERROR: Unable to access JVMTI!\n");
  }

  (void)memset(&capa, 0, sizeof(jvmtiCapabilities));
  capa.can_tag_objects = 1;
  error = (jvmti)->AddCapabilities(&capa);

  gdata = (GlobalAgentData*) malloc(sizeof(GlobalAgentData));
  gdata->jvmti = jvmti;
  return JNI_OK;
}

extern "C"
JNICALL jint objectCountingCallback(jlong class_tag, jlong size, jlong* tag_ptr, jint length, void* user_data) 
{
	int* count = (int*) user_data;
	*count += 1; 
	return JVMTI_VISIT_OBJECTS;
}

extern "C"
JNIEXPORT jint JNICALL Java_objectDeregister_ClassInstanceTest_countInstances(JNIEnv *env, jclass thisClass, jclass klass) 
{
	int count = 0;
	printf("force garbage collection\n");
        gdata->jvmti->ForceGarbageCollection();
	sleep(2);
  	printf("Getting objects\n");
	jvmtiHeapCallbacks callbacks;
  	(void)memset(&callbacks, 0, sizeof(callbacks));
	callbacks.heap_iteration_callback = &objectCountingCallback;
	jvmtiError error = gdata->jvmti->IterateThroughHeap(0, klass, &callbacks, &count);
	return count;
}
