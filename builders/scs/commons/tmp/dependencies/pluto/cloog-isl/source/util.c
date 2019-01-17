
/**-------------------------------------------------------------------**
 **                               CLooG                               **
 **-------------------------------------------------------------------**
 **                               util.c                              **
 **-------------------------------------------------------------------**
 **                   First version: January 8th 2014                 **
 **-------------------------------------------------------------------**/


/******************************************************************************
 *               CLooG : the Chunky Loop Generator (experimental)             *
 ******************************************************************************
 *                                                                            *
 * Copyright (C) 2002-2005 Cedric Bastoul                                     *
 *                                                                            *
 * This library is free software; you can redistribute it and/or              *
 * modify it under the terms of the GNU Lesser General Public                 *
 * License as published by the Free Software Foundation; either               *
 * version 2.1 of the License, or (at your option) any later version.         *
 *                                                                            *
 * This library is distributed in the hope that it will be useful,            *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of             *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU          *
 * Lesser General Public License for more details.                            *
 *                                                                            *
 * You should have received a copy of the GNU Lesser General Public           *
 * License along with this library; if not, write to the Free Software        *
 * Foundation, Inc., 51 Franklin Street, Fifth Floor,                         *
 * Boston, MA  02110-1301  USA                                                *
 *                                                                            *
 * CLooG, the Chunky Loop Generator                                           *
 * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                         *
 *                                                                            *
 ******************************************************************************/

#include <unistd.h>
#include <sys/time.h>
#include <stdio.h>
#include "../include/cloog/cloog.h"

/**
 * cloog_util_rtclock function:
 * this function returns the value, in seconds, of the real time clock of
 * the operating system. The reference point between calls is consistent,
 * making time comparison possible.
 * \return The real time clock of the operating system in seconds.
 */
double cloog_util_rtclock() {
    struct timezone Tzp;
    struct timeval Tp;
    int stat = gettimeofday(&Tp, &Tzp);
    if (stat != 0)
        cloog_msg(NULL, CLOOG_WARNING, "Error return from gettimeofday: %d", stat);
    return (Tp.tv_sec + Tp.tv_usec*1.0e-6);
}
