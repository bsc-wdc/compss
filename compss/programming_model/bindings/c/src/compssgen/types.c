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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "types.h"

void initTypes(Types *currTypes) {
    currTypes->num=0;
    currTypes->max=10;
    currTypes->types=malloc(currTypes->max*sizeof(Type));
}

void increaseTypesCapacity(Types *currTypes) {
    int oldMax = currTypes->max;
    currTypes->max = currTypes->max + 10;
    Type* tmp1 = malloc(currTypes->max*sizeof(Type));
    memcpy(tmp1, currTypes->types, oldMax*sizeof(Type));
    Type* tmp2 = currTypes->types;
    free(tmp2);
    currTypes->types = tmp1;
}

int containsType (Type type, Types currTypes) {
    int i;
    for (i=0; i<currTypes.num; i++) {
        if (currTypes.types[i].dt == type.dt && strcmp(currTypes.types[i].name,type.name)== 0 && strcmp(currTypes.types[i].elements, type.elements)==0) {
            return 1;
        }
    }
    return 0;
}

int getTypeNumber (Type type, Types currTypes) {
    int i;
    for (i=0; i<currTypes.num; i++) {
        if (currTypes.types[i].dt == type.dt && strcmp(currTypes.types[i].name,type.name)== 0 && strcmp(currTypes.types[i].elements, type.elements)==0) {
            return i;
        }
    }
    return -1;
}

void addType (Type type, Types *currTypes) {
    currTypes->num++;
    if (currTypes->num > currTypes->max) {
        increaseTypesCapacity(currTypes);
    }
    //currTypes->types[currTypes->num - 1]= (Type*)malloc(sizeof(Type));
    //memcpy(currTypes->types[currTypes->num - 1], type);
    currTypes->types[currTypes->num - 1]= type;

}

void printAllTypes(Types currTypes) {
    int i;
    printf("Printing all types(%d): \n", currTypes.num);
    for (i=0; i < currTypes.num ; i++) {
        printf("\tType %d: %s (type: %d, elements: %s)\n", i, currTypes.types[i].name, currTypes.types[i].dt, currTypes.types[i].elements);
    }

}
//int main()
int test_types() {
    Type a;
    a.dt=object_dt;
    a.elements="MAX";
    Type b;
    b.dt=object_dt;
    b.elements="MAX";
    Types currTypes;
    initTypes(&currTypes);
    printAllTypes(currTypes);
    strcpy( a.name, "Type1");
    addType(a, &currTypes);
    strcpy( a.name, "Type2");
    addType(a, &currTypes);
    strcpy( a.name, "Type3");
    addType(a, &currTypes);
    //Check print
    printAllTypes(currTypes);

    //Check if types exists
    strcpy(b.name, "Type1");
    if (containsType(b, currTypes)) {
        printf("Type %s in currTypes!\n", b.name);
    }
    strcpy(b.name, "Type2");
    if (containsType(b, currTypes)) {
        printf("Type %s in currTypes!\n", b.name);
    }
    strcpy(b.name, "Type3");
    if (containsType(b, currTypes)) {
        printf("Type %s in currTypes!\n", b.name);
    }
    strcpy(b.name, "Type4");
    if (!containsType(b, currTypes)) {
        printf("Type %s NOT in currTypes!\n",b.name);
    }
    addType(b, &currTypes);
    strcpy( a.name, "Type5");
    addType(a, &currTypes);
    strcpy( a.name, "Type6");
    addType(a, &currTypes);
    strcpy( a.name, "Type7");
    addType(a, &currTypes);
    strcpy( a.name, "Type8");
    addType(a, &currTypes);
    addType(b, &currTypes);
    strcpy( a.name, "Type9");
    addType(a, &currTypes);
    //Check increase
    strcpy( a.name, "Type10");
    addType(a, &currTypes);
    strcpy( a.name, "Type11");
    addType(a, &currTypes);
    strcpy( a.name, "Type12");
    addType(a, &currTypes);
    printAllTypes(currTypes);
    return 0;
}
