/*
 *  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
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
 */
#ifndef ABSTRACT_CACHE_H
#define ABSTRACT_CACHE_H

#include <map>
#include <pthread.h>

#include "JavaNioConnStreamBuffer.h"

#ifdef DEBUG_BINDING
#define debug_printf(args...) printf(args); fflush(stdout);
#else
#define debug_printf(args...) {}
#endif

using namespace std;

struct compss_pointer{
	void* pointer;
	int type;
	int elements;
	long size;
};

class AbstractCache{

private:
	pthread_mutex_t mtx;

protected:
	long max_size;
	long current_size;
	char* working_dir_path;
	map<string, compss_pointer> cache;
	int get_lock();
	int release_lock();

public:
	int pushToStream(const char* id, JavaNioConnStreamBuffer &jsb);
	int pullFromStream(const char* id, JavaNioConnStreamBuffer &jsb, compss_pointer &cp);
	int pushToFile(const char* id, const char* filename);
	int pullFromFile(const char* id, const char* filename, compss_pointer &cp);
	void init(long max_size, char* working_dir_path);
	bool isInCache(const char* id);
	int getFromCache(const char* id, compss_pointer &cp);
	int storeInCache(const char* id, compss_pointer cp);
	int deleteFromCache(const char* id, bool deleteObject);
	int copyInCache(const char* id_from, const char* id_to, compss_pointer &to);
	void printValues();

	static int removeData(const char* id, AbstractCache &c);
	static int serializeData(const char* id, const char* filename, AbstractCache &c);

	virtual int serializeToStream(compss_pointer cp, JavaNioConnStreamBuffer &jsb) = 0;
	virtual int deserializeFromStream(JavaNioConnStreamBuffer &jsb, compss_pointer &cp) = 0;
	virtual int serializeToFile(compss_pointer cp, const char* filename) = 0;
	virtual int deserializeFromFile(const char* filename, compss_pointer &cp) = 0;
	virtual int removeData(compss_pointer cp) = 0;
	virtual int copyData(compss_pointer form, compss_pointer &to) = 0;
};
#endif






