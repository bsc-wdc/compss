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
#ifndef COMMONS_JNI_H
#define COMMONS_JNI_H
#include <jni.h>
#include "common.h"

JNIEnv* create_vm(JavaVM ** jvm);

void destroy_vm(JavaVM * jvm);

int check_and_attach(JavaVM * jvm, JNIEnv *&env);

void check_and_treat_exception(JNIEnv *pEnv, const char*);

#endif // COMMONS_JNI_H
