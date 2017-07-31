
typedef struct {
  int max;
  char **types;
  int num;
} Types;

void initTypes(Types *currTypes);

int containsType (char* type, Types currTypes);

int getTypeNumber(char* type, Types currTypes);

void addType (char* type, Types *currTypes);

void printAllTypes(Types currTypes);
