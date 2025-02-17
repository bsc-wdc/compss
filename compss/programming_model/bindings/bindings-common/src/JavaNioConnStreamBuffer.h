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

#ifndef JAVA_NIO_BUFF_H
#define JAVA_NIO_BUFF_H

#include <streambuf>
#include <cstdlib>
#include <cstdio>
#include <jni.h>

class JavaNioConnStreamBuffer : public std::streambuf {
  private:
    char* buff;
    unsigned int size;
    jobject handle;
    jbyteArray j_value;
    JNIEnv* jni_env;
    std::size_t put_back_;
    unsigned int next_w_element;
  private:
    int_type underflow();
    int_type overflow(int_type in);

  public:
    //Ctor takes env pointer for the working thread and java.io.PrintStream
    JavaNioConnStreamBuffer(JNIEnv* env, jobject niostream, unsigned int buffsize);
    //JavaStreamBuffer(jobject niostream, unsigned int buffsize);
    ~JavaNioConnStreamBuffer();
};

#endif
