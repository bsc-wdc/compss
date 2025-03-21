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
#include <string.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <fstream>
#include "common_jni.h"

using namespace std;

JNIEnv* create_vm(JavaVM** jvm) {
    JNIEnv* env;
    JavaVMInitArgs vm_args;
    vector<JavaVMOption> options;

    string line; // buffer for line read
    debug_printf("[BINDING-COMMONS]  -  @create_vm  -  reading file in JVM_OPTIONS_FILE\n" );
    const char* file = strdup(getenv("JVM_OPTIONS_FILE")); // path to the file with jvm options
    ifstream fin; // input file stream

    fin.open(file);
    if (fin.good()) {
        while (!fin.eof()) {
            // read in one line at a time
            getline(fin, line);
            // read data from file
            string fileOption = strdup(line.data());
            if (fileOption != "" && fileOption.npos >= 0) {
                JavaVMOption* option = new JavaVMOption();
                string::size_type begin;
                string::size_type end;
                while ((begin = fileOption.find("$")) != fileOption.npos) {
                    // It refers to an environment variable
                    end = fileOption.find(":", begin);
                    if (end == fileOption.npos) {
                        end = fileOption.find("/", begin);
                    }
                    string prefix = fileOption.substr(0, begin);
                    string env_varName = fileOption.substr(begin + 1, end - begin - 1);

                    char* buffer = getenv(env_varName.data());
                    if (buffer == NULL) {
                        debug_printf("[BINDING-COMMONS]  -  @create_vm  -  Cannot find environment variable: %s\n", env_varName.data());
                    }

                    string env_varValue(buffer);
                    string suffix = "";
                    if (end != fileOption.npos) {
                        suffix = fileOption.substr(end);
                    }
                    fileOption.clear();
                    fileOption.append(prefix);
                    fileOption.append(env_varValue);
                    fileOption.append(suffix);
                }

                if (fileOption.find("-") == 0) {
                    // It is a JVM option
                    option->optionString = strdup(fileOption.data());
                    options.push_back(*option);
                    //Uncomment to debug JVM options errors
                    //debug_printf("[BINDING-COMMONS]  -  @create_vm  -  option %s\n", option->optionString);
                } else {
                    // It is an environment variable
                    //Uncomment to debug JVM options errors
                    debug_printf("[BINDING-COMMONS]  -  @create_vm  -  Putting environment variable\n");
                    int ret = putenv(strdup(fileOption.data()));
                    if (ret < 0) {
                        debug_printf("[BINDING-COMMONS]  -  @create_vm  -  Cannot put environment variable: %s", fileOption.data());
                    } else {
                        string::size_type begin = fileOption.find("=");
                        string env_varName = fileOption.substr(0, begin);
                        char *buffer = getenv(env_varName.data());
                        if (buffer == NULL)
                            debug_printf("[BINDING-COMMONS]  -  @create_vm  -  Cannot find environment variable: %s\n", env_varName.data());
                    }
                }
            }
            fflush(stdout);
            fileOption.clear();
        }
    } else {
        debug_printf("[BINDING-COMMONS]  -  @create_vm  -  JVM option file not good!\n");
    }
    // close file
    fin.close();

    vm_args.version = JNI_VERSION_1_8; //JDK version. This indicates version 1.8
    vm_args.nOptions = options.size();
    vm_args.options = new JavaVMOption[vm_args.nOptions];
    copy(options.begin(), options.end(), vm_args.options);
    vm_args.ignoreUnrecognized = false;
    debug_printf("[BINDING-COMMONS]  -  @create_vm  -  Launching JVM\n");
    int ret = JNI_CreateJavaVM(jvm, (void**) &env, &vm_args);
    if (ret < 0) {
        debug_printf("[BINDING-COMMONS]  -  @create_vm  -  Unable to Launch JVM - %i\n", ret);
        exit(1);
    } else {
        debug_printf("[BINDING-COMMONS]  -  @create_vm  -  JVM Ready\n");
    }
    return env;
}

void destroy_vm(JavaVM* jvm) {
    int ret = jvm->DestroyJavaVM();
    if (ret < 0) {
        debug_printf("[BINDINGS-COMMON]  -  @destroy_vm  -  Unable to Destroy JVM - %i\n", ret);
    }
}

int check_and_attach(JavaVM* jvm, JNIEnv* &env) {
    if (jvm == NULL){
		debug_printf("[BINDING-COMMONS]  -  @check_an_attach - No JVM provided.\n");
		exit(1);
	}
    int res = jvm->GetEnv((void **)&env, (int)JNI_VERSION_1_8);
    if (res == JNI_EDETACHED) {
        if (jvm->AttachCurrentThread((void **) &env, NULL) != 0) {
            printf("ERROR: Failed to attach thread to the JVM");
            fflush(NULL);
            return 0;
        } else {
            debug_printf("[BINDING-COMMONS]  -  @check_an_attach - Thread Attached to JVM.\n");
            return 1;
        }
    } else {
        // Already attached
        return 0;
    }
}

void _append_exception_trace_messages(JNIEnv& a_jni_env, std::string& a_error_msg, jthrowable a_exception,
                                      jmethodID a_mid_throwable_getCause, jmethodID  a_mid_throwable_getStackTrace,
                                      jmethodID a_mid_throwable_toString, jmethodID a_mid_frame_toString) {

    // Get the array of StackTraceElements.
    jobjectArray frames = (jobjectArray) a_jni_env.CallObjectMethod(a_exception, a_mid_throwable_getStackTrace);

    // Add Throwable.toString() before descending stack trace messages.
    if (frames != NULL) {
        jstring msg_obj = (jstring) a_jni_env.CallObjectMethod(a_exception, a_mid_throwable_toString);
        const char* msg_str = a_jni_env.GetStringUTFChars(msg_obj, 0);

        // If this is not the top-of-the-trace then this is a cause.
        if (!a_error_msg.empty()) {
            a_error_msg += "\nCaused by: ";
            a_error_msg += msg_str;
        } else {
            a_error_msg = msg_str;
        }

        a_jni_env.ReleaseStringUTFChars(msg_obj, msg_str);
        a_jni_env.DeleteLocalRef(msg_obj);

        // Append stack trace messages if there are any.
        jsize frames_length = a_jni_env.GetArrayLength(frames);
        if (frames_length > 0) {
            jsize i = 0;
            for (i = 0; i < frames_length; i++) {
                // Get the string returned from the 'toString()' method of the next frame and append it to
                // the error message.
                jobject frame = a_jni_env.GetObjectArrayElement(frames, i);
                jstring msg_obj =
                    (jstring) a_jni_env.CallObjectMethod(frame,
                            a_mid_frame_toString);

                const char* msg_str = a_jni_env.GetStringUTFChars(msg_obj, 0);

                a_error_msg += "\n    ";
                a_error_msg += msg_str;

                a_jni_env.ReleaseStringUTFChars(msg_obj, msg_str);
                a_jni_env.DeleteLocalRef(msg_obj);
                a_jni_env.DeleteLocalRef(frame);
            }
        }
        // If 'a_exception' has a cause then append the stack trace messages from the cause.
        jthrowable cause = (jthrowable) a_jni_env.CallObjectMethod(a_exception, a_mid_throwable_getCause);
        if (0 != cause) {
            _append_exception_trace_messages(a_jni_env,
                                             a_error_msg,
                                             cause,
                                             a_mid_throwable_getCause,
                                             a_mid_throwable_getStackTrace,
                                             a_mid_throwable_toString,
                                             a_mid_frame_toString);
        }
    }
}

void check_and_treat_exception(JNIEnv* pEnv, const char* message) {
    jthrowable exception = pEnv->ExceptionOccurred();
    if (exception) {
        // Log provided exception message
        printf("\n[BINDING-COMMONS] Exception: %s. \n", message);

        // Log JNI Exception
        pEnv->ExceptionDescribe();

        // Log detailed exception
        jclass throwable_class = pEnv->FindClass("java/lang/Throwable");
        jmethodID mid_throwable_getCause = pEnv->GetMethodID(throwable_class, "getCause", "()Ljava/lang/Throwable;");
        jmethodID mid_throwable_getStackTrace = pEnv->GetMethodID(throwable_class, "getStackTrace",     "()[Ljava/lang/StackTraceElement;");
        jmethodID mid_throwable_toString = pEnv->GetMethodID(throwable_class, "toString", "()Ljava/lang/String;");
        jclass frame_class = pEnv->FindClass("java/lang/StackTraceElement");
        jmethodID mid_frame_toString = pEnv->GetMethodID(frame_class, "toString", "()Ljava/lang/String;");
        std::string error_msg;
        _append_exception_trace_messages(*pEnv, error_msg, exception, mid_throwable_getCause, mid_throwable_getStackTrace,
                                         mid_throwable_toString, mid_frame_toString);
        printf("\n[BINDING-COMMONS] Exception Occurred during runtime interaction:\n %s", error_msg.c_str());

        // Error exit
        exit(1);
    }
}
