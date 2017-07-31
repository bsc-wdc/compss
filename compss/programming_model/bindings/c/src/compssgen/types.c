#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "types.h"

void initTypes(Types *currTypes){
  currTypes->num=0;
  currTypes->max=10;
  currTypes->types=malloc(currTypes->max*sizeof(char*));
}  

void increaseTypesCapacity(Types *currTypes){
  int oldMax = currTypes->max;
  currTypes->max = currTypes->max + 10;
  char** tmp1 = malloc(currTypes->max*sizeof(char*));
  memcpy(tmp1,currTypes->types, oldMax*sizeof(char*));
  char** tmp2 = currTypes->types;
  free(tmp2);
  currTypes->types = tmp1;
}

int containsType (char* type, Types currTypes){
  int i;
  for (i=0; i<currTypes.num; i++){
	if (strcmp(currTypes.types[i],type)==0){
		return 1;
	}
  }
  return 0;
}

int getTypeNumber (char* type, Types currTypes){
  int i;
  for (i=0; i<currTypes.num; i++){
        if (strcmp(currTypes.types[i],type)==0){
                return i;
        }
  }
  return -1;
}

void addType (char* type, Types *currTypes){
  currTypes->num++; 
  if (currTypes->num > currTypes->max){
     increaseTypesCapacity(currTypes);  
  }	
  currTypes->types[currTypes->num - 1]= malloc( sizeof(char*));
  strcpy(currTypes->types[currTypes->num - 1], type); 
  
}

void printAllTypes(Types currTypes){
  int i;
  printf("Printing all types(%d): \n", currTypes.num);
  for (i=0; i < currTypes.num ; i++){
        printf("\tType %d: %s\n", i, currTypes.types[i]);
  }
  
}
//int main() 
int test_types()
{    
  char *a = malloc (sizeof(char*));
  char *b = malloc (sizeof(char*));
  Types currTypes;
  initTypes(&currTypes);
  printAllTypes(currTypes);
  strcpy( a, "Type1");
  addType(a, &currTypes);
  strcpy( a, "Type2");
  addType(a, &currTypes);
  strcpy( a, "Type3");
  addType(a, &currTypes);
  //Check print
  printAllTypes(currTypes);

  //Check if types exists
  strcpy(b, "Type1");
  if (containsType(b, currTypes)){
	printf("Type %s in currTypes!\n", b);
  }
  strcpy(b, "Type2");
  if (containsType(b, currTypes)){
        printf("Type %s in currTypes!\n", b);
  }
  strcpy(b, "Type3");
  if (containsType(b, currTypes)){
        printf("Type %s in currTypes!\n", b);
  }
  strcpy(b, "Type4");
  if (!containsType(b, currTypes)){
        printf("Type %s NOT in currTypes!\n",b);
  }
  addType(b, &currTypes);
  strcpy( a, "Type5");
  addType(a, &currTypes);
  strcpy( a, "Type6");
  addType(a, &currTypes);
  strcpy( a, "Type7");
  addType(a, &currTypes);
  strcpy( a, "Type8");
  addType(a, &currTypes);
  addType(b, &currTypes);
  strcpy( a, "Type9");
  addType(a, &currTypes);
  //Check increase
  strcpy( a, "Type10");
  addType(a, &currTypes);
  strcpy( a, "Type11");
  addType(a, &currTypes);
  strcpy( a, "Type12");
  addType(a, &currTypes); 
  printAllTypes(currTypes);
  return 0;
}
