/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

#include "common.h"
#include "common_jni.h"
#include "tracing_interface.h"
#include "tracing_jni.h"


jclass globalTracer;
jclass tracer_clsString;

jmethodID midStartSynchronization;  /* ID of the startSynchronization method in the es.bsc.compss.util.Tracer class */
jmethodID midEndSynchronization;    /* ID of the endSynchronization method in the es.bsc.compss.util.Tracer class */
jmethodID midActiveComponent;       /* ID of the activeComponent method in the es.bsc.compss.util.Tracer class */
jmethodID midInactiveComponent;     /* ID of the inactiveComponent method in the es.bsc.compss.util.Tracer class */
jmethodID midDefineNewEventType;        /* ID of the defineNewEventType method in the es.bsc.compss.util.Tracer class */
jmethodID midEmitEvent;                 /* ID of the EmitEvent method in the es.bsc.compss.util.Tracer class */


void JNI_StartSynchronization(long value) {
    debug_printf("[BINDING-COMMONS] - @JNI_StartSynchronization - Start Synchronization\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    jlong jValue = (jlong) value;

    // Perform operation
    debug_printf("[BINDING-COMMONS] - @JNI_StartSynchronization - Value: %ld\n", value);
    status->localJniEnv->CallStaticVoidMethod(globalTracer, midStartSynchronization, jValue);
    check_exception(status, "Exception received when calling startSynchronization");

    // Revoke thread access to JVM
    access_revoke(status);
    
    debug_printf("[BINDING-COMMONS] - @JNI_StartSynchronization - Start Synchronization emitted\n");
}

void JNI_EndSynchronization() {
    debug_printf("[BINDING-COMMONS] - @JNI_EndSynchronization - End Synchronization\n");
    
    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallStaticVoidMethod(globalTracer, midEndSynchronization);
    check_exception(status, "Exception received when calling endSynchronization");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_EndSynchronization - End Synchronization emitted\n");
}

void JNI_ActiveComponent(int id, char* description) {
    debug_printf("[BINDING-COMMONS] - @JNI_ActiveComponent - Active Component\n");
	if (id < 0 || description == NULL) {
        debug_printf("[BINDING-COMMONS] - @JNI_ActiveComponent - Error: Invalid parameters\n");
        return;
    }

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    jint jId = (jint) id;
    jstring jDescription = status->localJniEnv->NewStringUTF(description);

    // Perform operation
    debug_printf("[BINDING-COMMONS] - @JNI_ActiveComponent - ID: %d, Description: %s\n", id, description);
    status->localJniEnv->CallStaticVoidMethod(globalTracer, midActiveComponent, jId, jDescription);
    check_exception(status, "Exception received when calling activeComponent");

    // Clean up local references
    status->localJniEnv->DeleteLocalRef(jDescription);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_ActiveComponent - Active Component emitted\n");
}

void JNI_InactiveComponent() {
    debug_printf("[BINDING-COMMONS] - @JNI_InactiveComponent - Inactive Component\n");

    // Request thread access to JVM
    ThreadStatus* status = access_request();

    // Perform operation
    status->localJniEnv->CallStaticVoidMethod(globalTracer, midInactiveComponent);
    check_exception(status, "Exception received when calling inactiveComponent");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_InactiveComponent - Inactive Component emitted\n");
}


void JNI_EmitEvent(int type, long id) {
    debug_printf("[BINDING-COMMONS] - @JNI_EmitEvent - Emit Event\n");

    // Check validity
    if (type < 0  or id < 0) {
        debug_printf ("[BINDING-COMMONS] - @JNI_EmitEvent - Error: event type and ID must be positive integers, but found: type: %u, ID: %lu\n", type, id);
        return;
    }

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    jint jType = (jint) type;
    jlong jId = (jlong) id;

    // Perform operation
    debug_printf ("[BINDING-COMMONS] - @JNI_EmitEvent - Type: %u, ID: %lu\n", type, id);
    status->localJniEnv->CallStaticVoidMethod(globalTracer, midEmitEvent, jType, jId);
    check_exception(status, "Exception received when calling emitEvent");

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_EmitEvent - Event emitted\n");
}


void JNI_DefineNewEventType(int code, char* description, int endable, int numEvents, int* eventIDs, char** eventLabels) {
    debug_printf("[BINDING-COMMONS] - @JNI_DefineNewEventType - Define Event Type\n");

    // Check validity
    if (code < 0 || description == NULL || numEvents < 0) {
        debug_printf("[BINDING-COMMONS] - @JNI_DefineNewEventType - Error: Invalid parameters\n");
        return;
    }

    // Request thread access to JVM
    ThreadStatus* status = access_request();
    jint jCode = (jint)code;
    jstring jDescription = status->localJniEnv->NewStringUTF(description);
    jboolean jEndable = (jboolean)(endable ? JNI_TRUE : JNI_FALSE);
    
    // Create Java int array for eventIDs
    jintArray jEventIDs = status->localJniEnv->NewIntArray(numEvents);
    if (eventIDs != NULL && numEvents > 0) {
        status->localJniEnv->SetIntArrayRegion(jEventIDs, 0, numEvents, (const jint*)eventIDs);
    }

    // Create Java String array for eventLabels
    jobjectArray jEventLabels = status->localJniEnv->NewObjectArray(numEvents, 
                                                                     tracer_clsString, 
                                                                     status->localJniEnv->NewStringUTF(""));
    if (eventLabels != NULL && numEvents > 0) {
        for (int i = 0; i < numEvents; i++) {
            jstring label = status->localJniEnv->NewStringUTF(eventLabels[i]);
            status->localJniEnv->SetObjectArrayElement(jEventLabels, i, label);
            status->localJniEnv->DeleteLocalRef(label);
        }
    }

    // Perform operation
    debug_printf("[BINDING-COMMONS] - @JNI_DefineNewEventType - Code: %d, Description: %s, Endable: %d, NumIDs: %d\n", 
                 code, description, endable, numEvents);
    jobject result = status->localJniEnv->CallStaticObjectMethod(globalTracer, midDefineNewEventType, 
                                                                  jCode, jDescription, jEndable, jEventIDs, jEventLabels);
    check_exception(status, "Exception received when calling defineNewEventType");

    // Extract event type code from the returned EventType object (assuming it's an enum with ordinal)
    if (result != NULL) {
        status->localJniEnv->DeleteLocalRef(result);
    }

    // Clean up local references
    status->localJniEnv->DeleteLocalRef(jDescription);
    status->localJniEnv->DeleteLocalRef(jEventIDs);
    status->localJniEnv->DeleteLocalRef(jEventLabels);

    // Revoke thread access to JVM
    access_revoke(status);

    debug_printf("[BINDING-COMMONS] - @JNI_DefineNewEventType - Event type defined with code: %d\n", code);
    return;
}


TracingInterface setup_JNI_tracing(){

    // Create the JVM instance
    debug_printf ("[BINDING-COMMONS] - @JNI_On - Creating the JVM\n");
    create_vm();// Probably, it will be already created by the COMPSs runtime, but we need to ensure it is created before accessing it.

    // Request thread access to JVM
    // debug_printf ("[BINDING-COMMONS] - @JNI_On - Request thread access to JVM\n");
    ThreadStatus* status = access_request();


    // Obtain Tracer class
    debug_printf ("[BINDING-COMMONS] - @JNI_On - Obtaining Tracer class\n");  
    jclass clsLocal = status->localJniEnv->FindClass("es/bsc/compss/util/Tracer");
    check_exception(status, "Cannot find the Tracer class");
    globalTracer = (jclass) status->localJniEnv->NewGlobalRef(clsLocal);
    check_exception(status, "Cannot instantiate the Tracer class");
   
    // Get StartSynchronization method ID
    midStartSynchronization = status->localJniEnv->GetStaticMethodID(globalTracer, "startSynchronization", "(J)V");
    check_exception(status, "Cannot find startSynchronization");

    // Get EndSynchronization method ID
    midEndSynchronization = status->localJniEnv->GetStaticMethodID(globalTracer, "endSynchronization", "()V");
    check_exception(status, "Cannot find endSynchronization");

    // Get ActiveComponent method ID
    midActiveComponent = status->localJniEnv->GetStaticMethodID(globalTracer, "activeComponent", "(ILjava/lang/String;)V");
    check_exception(status, "Cannot find activeComponent");

    // Get InactiveComponent method ID
    midInactiveComponent = status->localJniEnv->GetStaticMethodID(globalTracer, "inactiveComponent", "()V");
    check_exception(status, "Cannot find inactiveComponent");

    // Get EmitEvent method ID
    midEmitEvent = status->localJniEnv->GetStaticMethodID(globalTracer, "emitEvent", "(IJ)V");
    check_exception(status, "Cannot find emitEvent");

    // Get DefineNewEventType method ID
    midDefineNewEventType = status->localJniEnv->GetStaticMethodID(globalTracer, "defineNewEventType", 
                                                                    "(ILjava/lang/String;Z[I[Ljava/lang/String;)Les/bsc/wdc/tracing/EventType;");
    check_exception(status, "Cannot find defineNewEventType");
    
    // Obtain String class
    debug_printf("[BINDING-COMMONS] - @setup_JNI_tracing - Obtaining String class\n");
    jclass strClsLocal = status->localJniEnv->FindClass("java/lang/String");
    check_exception(status, "Cannot find the String class");
    tracer_clsString = (jclass) status->localJniEnv->NewGlobalRef(strClsLocal);
    check_exception(status, "Cannot instantiate the String class reference");
    
    // Revoke thread access to JVM
    // debug_printf ("[BINDING-COMMONS] - @JNI_On - Revoke thread access to JVM\n");
    access_revoke(status);

    TracingInterface iface{};
    
    iface.StartSynchronization = JNI_StartSynchronization;
    iface.EndSynchronization = JNI_EndSynchronization;

    iface.ActiveComponent = JNI_ActiveComponent;
    iface.InactiveComponent = JNI_InactiveComponent;

    iface.DefineNewEventType = JNI_DefineNewEventType;

    iface.EmitEvent = JNI_EmitEvent;
    return iface;
}
