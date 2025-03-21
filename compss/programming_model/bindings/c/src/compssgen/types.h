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

#include <param_metadata.h>

typedef struct {
    char* name;
    char* elements;
    enum datatype dt;
} Type;

typedef struct {
    int max;
    Type *types;
    int num;
} Types;

void initTypes(Types *currTypes);

int containsType (Type type, Types currTypes);

int getTypeNumber(Type type, Types currTypes);

void addType (Type type, Types *currTypes);

void printAllTypes(Types currTypes);
