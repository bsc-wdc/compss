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
