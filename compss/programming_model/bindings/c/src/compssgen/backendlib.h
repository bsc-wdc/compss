#ifndef BACKENDLIB_H
#define BACKENDLIB_H

#include <stdio.h>

void replace_char (char *s, char find, char replace);
void rename_if_clash(char *origin);
FILE *create_without_overwrite(char *origin);
char const *get_filename_base();
void set_no_backups();
unsigned long hash(char *str);
#endif /* BACKENDLIB_H */
