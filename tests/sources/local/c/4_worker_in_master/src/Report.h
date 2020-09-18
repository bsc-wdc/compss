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
#ifndef REPORT_H
#define REPORT_H

#include <cstddef>
#include <vector>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/array.hpp>

#include <iostream>

using namespace std;
using namespace boost;
using namespace serialization;



class Report {

public:
    Report();

    long int get_start_time();

    void set_start_time(long int start_time);

    long int get_end_time();

    void set_end_time(long int end_time);

private:
    long int start_time;
    long int end_time;

    friend class::serialization::access;

    template<class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & start_time;
        ar & end_time;
    }
    
};

#endif