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
#include <streambuf>
#include <cstdlib>
#include <cstdio>
#include <algorithm>
#include <cstring>
#include <iostream>

using std::size_t;
#include "common.h"
#include "JavaNioConnStreamBuffer.h"

//Ctor takes env pointer for the working thread and java.io.PrintStream
JavaNioConnStreamBuffer::JavaNioConnStreamBuffer(JNIEnv* jni_env, jobject  niostream, unsigned int buffsize) {
    this->jni_env = jni_env;
    this->handle = jni_env->NewGlobalRef(niostream);
    this->size = buffsize;
    this->put_back_= size_t(1);
    this->buff= new char[buffsize];
    this->next_w_element=0;
    this->j_value = NULL;
    char *end = &buff[0] + size;
    setg(end, end, end);
}

//This method  the central output of the streambuf class, every charakter goes here
std::streambuf::int_type JavaNioConnStreamBuffer::overflow(std::streambuf::int_type in = traits_type::eof()) {
    //printf(" [JSB-OF] Calling overflow %d \n", in);
    fflush(NULL);
    buff[next_w_element]=in;
    next_w_element++;
    if(in == std::ios::traits_type::eof() ||  next_w_element == size) {
        jobject o = jni_env->NewDirectByteBuffer(buff,sizeof(char)*next_w_element);
        debug_printf(" [JSB-OF] Calling push method to transfer data\n");
        jmethodID id = jni_env->GetMethodID(jni_env->GetObjectClass(handle),"push","(Ljava/nio/ByteBuffer;)V");
        if (o !=NULL && id!=NULL) {
            jni_env->CallVoidMethod(handle,id,o);
            if (jni_env->ExceptionOccurred()) {
                print_error(" [JSB-OF] Exception calling push method \n");
                jni_env->ExceptionDescribe();
                exit(0);
            }
            buff[0]=in;
            next_w_element=0;
        } else {
            print_error("[JSB-OF] Error: PUSH Method ID is null");
            exit(0);
        }

    }
    return in;
}
std::streambuf::int_type JavaNioConnStreamBuffer::underflow() {
    if (gptr() < egptr()) { // buffer not exhausted
        std::streambuf::int_type out = traits_type::to_int_type(*gptr());
        /*printf(" [JSB-UF] Returning %d\n", out);
        fflush(NULL);*/
        return out;
    }

    // start is now the start of the buffer, proper.
    // Read from fptr_ in to the provided buffer
    debug_printf(" [JSB-UF] Calling pull method to get data\n");
    fflush(NULL);
    if (j_value != NULL) {
        debug_printf(" [JSB-UF] Releasing previous bytearray (%p,%p)\n", &j_value, buff);
        jni_env->ReleaseByteArrayElements(j_value, (jbyte*)buff, 0);
    }
    jmethodID id = jni_env->GetMethodID(jni_env->GetObjectClass(handle),"pull","()[B");
    if (id != NULL) {
        j_value = (jbyteArray)jni_env->CallObjectMethod(handle,id);
        if (jni_env->ExceptionOccurred()) {
            print_error(" [JSB-UF] Exception calling method \n");
            jni_env->ExceptionDescribe();
            exit(0);
        }
        if(j_value != NULL) {
            size = jni_env->GetArrayLength(j_value);
            if (size == 0) {
            	debug_printf(" [JSB-UF] Underflow returning EOF \n");
                return traits_type::eof();
            }
            buff = (char*) jni_env->GetByteArrayElements(j_value, NULL);
            // Set buffer pointers
            char *base = buff;
            char *start = base;
            char *end = base + size;
            fflush(NULL);
            setg(base, start, end);
            std::streambuf::int_type out = std::ios::traits_type::to_int_type(buff[0]);
            /*printf(" [JSB-UF] Returning (pointer %p) %d\n", buff, out);
            fflush(NULL);*/
            return out;
        } else {
            debug_printf(" [JSB-UF] Underflow returning null, sending EOF \n");
            return traits_type::eof();
        }

    } else {
        print_error("Error: PULL Method ID is null");
        exit(0);
    }
}

JavaNioConnStreamBuffer::~JavaNioConnStreamBuffer() {
    if (j_value != NULL) {
        printf(" [JSB-UF] Releasing previous bytearray (%p,%p) \n", &j_value, buff);
        jni_env->ReleaseByteArrayElements(j_value, (jbyte*)buff, 0);
    } else {
        overflow();
    }
    printf(" [JSB-UF] Releasing handle (%p) \n", &handle);
    jni_env->DeleteGlobalRef(handle);
}
