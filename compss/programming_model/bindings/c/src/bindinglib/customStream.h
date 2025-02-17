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
#include <fstream>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/syscall.h>

#define gettid() syscall(SYS_gettid)

#ifndef CUSTOMSTREAM_H
#define CUSTOMSTREAM_H

using namespace std;

class customStream : public streambuf {

  private:
    map<int, streambuf*> files;
    streambuf* defaultBuf;
    pthread_mutex_t mtx;
    streamsize xsputn(const char* s, streamsize n) {
        if (files.find(gettid()) != files.end()) {
            files[gettid()]->sputn(s,n);
        } else {
            defaultBuf->sputn(s,n);
        }
        return n;
    };

    int overflow (int c) {
        int ret;
        if (files.find(gettid()) != files.end()) {
            ret = files[gettid()]->sputc(c);
        } else {
            ret = defaultBuf->sputc(c);
        }
        return ret;
    };

    // This function is called when a flush is called by endl, adds an endline to the output
    int sync() {
        int ret;
        if (files.find(gettid()) != files.end()) {
            ret = files[gettid()]->pubsync();
        } else {
            ret = defaultBuf->pubsync();
        }
        return ret;
    };

  public :

    customStream(streambuf* defsb) {
        defaultBuf = defsb;
        pthread_mutex_init(&mtx,NULL);
    };

    //customStream(const char * data, unsigned int len);

    void registerThread(streambuf* threadsb) {
        pthread_mutex_lock(&mtx);
        files[gettid()] = threadsb;
        pthread_mutex_unlock(&mtx);
    };

    void unregisterThread() {
        pthread_mutex_lock(&mtx);
        files.erase(gettid());
        pthread_mutex_unlock(&mtx);
    };
};
#endif
