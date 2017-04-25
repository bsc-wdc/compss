#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <stdio.h>

#ifdef HAVE_STRING_H
#include <string.h>
#endif
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

#include <limits.h>
#include "backendlib.h"
#include "semantic.h"


static int do_backups=1;


void replace_char (char *s, char find, char replace) {
    while (*s != 0) {
        if (*s == find)
        	*s = replace;
        s++;
    }
}

void rename_if_clash(char *origin)
{
	char dest[PATH_MAX];
	FILE *tmp;
	
	if (do_backups && (tmp = fopen(origin, "r")) != NULL) {
		fclose(tmp);
		printf("Warning: renaming file '%s' to '%s~'.\n", origin, origin);
		strncpy(dest, origin, PATH_MAX);
		strncat(dest, "~", PATH_MAX);
		rename(origin, dest);
	}
}


FILE *create_without_overwrite(char *origin)
{
	FILE *tmp, *result;
	
	if (do_backups && (tmp = fopen(origin, "r")) != NULL) {
		fclose(tmp);
		printf("Warning: the file '%s' will not be overwritten.\n", origin);
		result = fopen("/dev/null", "w");
	} else {
		result = fopen(origin, "w");
	}
	
	return result;
}


char const *get_filename_base()
{
	char *ext;
	static char name[PATH_MAX];
	
	ext = strrchr(get_filename(), '.');
	strncpy(name, get_filename(), ext - get_filename());
	name[ext - get_filename()] = 0;

	return name;
}


void set_no_backups()
{
	do_backups = 0;
}


