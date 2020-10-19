#include<string.h>
#include<stdlib.h>
#include<jni.h>
#include<jvmti.h>
using namespace std;

jvmtiEnv *jvmti; //java VM environment

JNIEXPORT jint JNICALL Agent_OnLoad(JavaVM *jvm, char *options, void *reserved) {
	jvmtiEnv *tmp = NULL;
	jvmtiCapabilities capa;
	jvmtiError error;

	jint result = jvm->GetEnv((void **) &tmp, JVMTI_VERSION_1_1);
	if (result != JNI_OK) {
		printf("ERROR: Cannot load JVMTIENV\n");
	}

	(void)memset(&capa, 0, sizeof(jvmtiCapabilities)); //Fill with 0's
	capa.can_tag_objects = 1;			   //Activate the desired capabilities with 1's

	error = (tmp)->AddCapabilities(&capa);

	jvmti = tmp;

	return JNI_OK;
}

extern "C"
JNICALL jint objectCountingCallback(jlong class_tag, jlong size, jlong* tag_ptr, jint length, void* user_data) {
	int* n = (int*) user_data;
	*n += 1;

	return JVMTI_VISIT_OBJECTS; //0x100 value (256)

	//This function will be set as a callback in every heap iteration,
	//every object will be counted.
	
}

extern "C"
JNIEXPORT jint JNICALL Java_ObjectDeregister_countInstances(JNIEnv *env, jclass thisClass, jclass klass) {
	int count = 0;
        printf("force garbage collection\n");
        gdata->jvmti->ForceGarbageCollection();
        sleep(2);

	jvmtiHeapCallbacks callbacks;
	(void)memset(&callbacks, 0, sizeof(callbacks));
	callbacks.heap_iteration_callback = &objectCountingCallback; //We activate the heap iteration callback with the function defined above
	jvmtiError error = jvmti->IterateThroughHeap(0, klass, &callbacks, &count); //And we iterate through the heap searching for the class "klass"

	return count;
}


