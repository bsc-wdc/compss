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
#include <string>
#include "common.h"
#include "AbstractCache.h"


void AbstractCache::init(long max_size, char* working_dir_path) {
    this->max_size = max_size;
    this->current_size = 0;
    this->working_dir_path = working_dir_path;
}

int AbstractCache::get_lock() {
    return pthread_mutex_lock(&mtx);
}

int AbstractCache::release_lock() {
    return pthread_mutex_unlock(&mtx);
}

int AbstractCache::pushToStream(const char* id, JavaNioConnStreamBuffer &jsb) {
    if (is_debug()) {
        cout << "[AbstractCache] Pushing Object "<< id << " from cache to stream." << endl;
    }
    compss_pointer cp;
    int res = getFromCache(id, cp);
    if (res == 0) {
        return serializeToStream(cp,jsb);
    } else {
        cout << "[AbstractCache] Data " << id << " not found in cache." << endl;
        printValues();
        return -1;
    }
}

int AbstractCache::pullFromStream(const char* id, JavaNioConnStreamBuffer &jsb, compss_pointer &cp) {
    if (is_debug()) {
        cout << "[AbstractCache] Getting Object "<< id << " from stream." << endl;
    }
    int res = deserializeFromStream(jsb, cp);
    if (res==0) {
        if (is_debug()) {
            cout << "[AbstractCache] Storing deserialized object to cache as "<< id << endl;
        }
        return storeInCache(id,cp);
    } else {
        cout << "[AbstractCache] Error deserializing "<< id << " from stream." << endl;
        printValues();
        return -1;
    }
}

int AbstractCache::pushToFile(const char* id, const char* filename) {
    if (is_debug()) {
        cout << "[AbstractCache] Pushing Object "<< id << " from cache to file " << filename <<"." << endl;
    }
    compss_pointer cp;
    int res = getFromCache(id, cp);
    if (res == 0) {
        return serializeToFile(cp,filename);
    } else {
        cout << "[AbstractCache] Data " << id << " not found in cache." << endl;
        printValues();
        return -1;
    }
}

int AbstractCache::pullFromFile(const char* id, const char* filename, compss_pointer &cp) {
    if (is_debug()) {
        cout << "[AbstractCache] Getting Object "<< id << " from file "<< filename << "." << endl;
    }
    int res = deserializeFromFile(filename, cp);
    if (res == 0) {
        storeInCache(id,cp);
        return 0;
    } else {
        cout << "[AbstractCache] Error deserializing from file " << filename << endl;
        printValues();
        return -1;
    }
}

bool AbstractCache::isInCache(const char* id) {
	if (is_debug()) {
	    cout << "[AbstractCache] Checking if object "<< id << " is in cache." << endl;
	}
	get_lock();
    bool b = cache.count(id) > 0;
    release_lock();
    return b;

}

int AbstractCache::getFromCache(const char* id, compss_pointer &cp) {
	if (is_debug()) {
		    cout << "[AbstractCache] Getting object "<< id << " from cache." << endl;
		}
	if (isInCache(id)) {
        get_lock();
        cp = cache[id];
        release_lock();
        return 0;
    } else {
        return -1;
    }
}

int AbstractCache::storeInCache(const char* _id, compss_pointer cp) {
	string id(_id);
	if (is_debug()) {
		cout << "[AbstractCache] Storing object "<< id << " in cache. " << hex << cp.pointer << endl;
	}
	get_lock();
    cache[id]=cp;
    release_lock();
    return 0;
}
//Only removes from the cache, binding or user have to remove the object from memory

int AbstractCache::deleteFromCache(const char* id, bool deleteObject) {
	if (is_debug()) {
			cout << "[AbstractCache] Deleting object " << id << " from cache." << endl;
		}
	compss_pointer cp;
    int res = getFromCache(id, cp);
    if (res==0) {
        get_lock();
        cache.erase(id);
        release_lock();
        if (deleteObject) {
            return removeData(cp);
        } else {
            return 0;
        }
    } else {
        cout << "[AbstractCache] Data " << id << " not found in cache." << endl;
        printValues();
        return res;
    }
}

int AbstractCache::copyInCache(const char* id_from, const char* id_to, compss_pointer &to) {
	if (is_debug()) {
		cout << "[AbstractCache] Copying object " << id_from << " to " << id_to << endl;
	}
	compss_pointer from;
    int res = getFromCache(id_from, from);
    if (res==0) {
        int res_cp = copyData(from, to);
        if (res_cp==0) {
            return storeInCache(id_to, to);
        } else {
            cout << "[AbstractCache] Error copying data " << id_from << endl;
            return res_cp;
        }
    } else {
        cout << "[AbstractCache] Data " << id_from << " not found in cache." << endl;
        printValues();
        return res;
    }
}

int AbstractCache::moveInCache(const char* id_from, const char* id_to) {
	if (is_debug()) {
		cout << "[AbstractCache] Moving object " << id_from << " to " << id_to << endl;
	}
	compss_pointer from;
    int res = getFromCache(id_from, from);
    if (res==0) {
        get_lock();
        cache.erase(id_from);
        release_lock();
        return storeInCache(id_to, from);
    } else {
        cout << "[AbstractCache] Data " << id_from << " not found in cache." << endl;
        printValues();
        return res;
    }
}

void AbstractCache::printValues() {
	if (is_debug()) {
		get_lock();
		cout << "[AbstractCache] Cache contains:" <<endl;
		for(std::map<std::string, compss_pointer>::iterator it = cache.begin(); it != cache.end(); it++) {
			cout << it->first << " " << hex << it->second.pointer << endl;
		}
		release_lock();
	}
}

int AbstractCache::removeData(const char* id, AbstractCache &c) {
    if (is_debug()) {
        cout << "[AbstractCache] Deleting from cache data " << id  << endl;
    }
    bool found = c.isInCache(id);
    if (found) {
        int res = c.deleteFromCache(id, false);
        if (res != 0) {
            cout << "[C Binding] Error deleting data " << id << endl;
            return res;
        }
        return 0;
    } else {
        cout << "[C Binding] Error data " << id  << " not found in cache." << endl;
        return -1;
    }
}

int AbstractCache::serializeData(const char* id, const char* filename, AbstractCache &c) {
    if (is_debug()) {
        cout << "[AbstractCache] Serializing from cache data " << id  << endl;
    }
    bool found = c.isInCache(id);
    if (found) {
        int res = c.pushToFile(id, filename);
        if (res != 0) {
            cout << "[C Binding] Error serializing data " << id  << " to file " << filename << endl;
            return res;
        }
        return 0;
    } else {
        cout << "[C Binding] Error data " << id  << " not found in cache." << endl;
        return -1;
    }
}






