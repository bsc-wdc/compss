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
#include "Report.h"

Report::Report() {
}

long int Report::get_start_time(){
    return this->start_time;
}

void Report::set_start_time(long int start_time){
    this->start_time = start_time;
}

long int Report::get_end_time(){
    return this->end_time;
}

void Report::set_end_time(long int end_time){
    this->end_time = end_time;
}