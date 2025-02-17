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

#include <AbstractCache.h>
#ifndef CBINDING_CACHE_H
#define CBINDING_CACHE_H
class CBindingCache: public AbstractCache {

  public:

    CBindingCache():AbstractCache() {};

    ~CBindingCache() {};

    int serializeToStream(compss_pointer cp, JavaNioConnStreamBuffer &jsb);

    int deserializeFromStream(JavaNioConnStreamBuffer &jsb, compss_pointer &cp);

    int serializeToFile(compss_pointer cp, const char* filename);

    int deserializeFromFile(const char* filename, compss_pointer &cp);

    int removeData(compss_pointer cp);

    int copyData(compss_pointer from, compss_pointer &to);

};
#endif





