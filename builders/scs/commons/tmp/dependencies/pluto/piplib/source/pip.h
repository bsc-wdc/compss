#include <piplib/piplib.h>
#include <stdio.h> /* various function declarations use FILE */
#include "type.h"
#include "sol.h"
#include "tab.h"
#include "funcall.h"

#define dscanf_xx PIPLIB_NAME(dscanf)
int dscanf_xx(FILE*, piplib_int_t_xx*);
