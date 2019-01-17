#!/bin/sh
#
#   /**-------------------------------------------------------------------**
#    **                              CLooG                                **
#    **-------------------------------------------------------------------**
#    **                           checker.sh                              **
#    **-------------------------------------------------------------------**
#    **                 First version: November 16th 2011                 **
#    **-------------------------------------------------------------------**/
#

#/*****************************************************************************
# *               CLooG : the Chunky Loop Generator (experimental)            *
# *****************************************************************************
# *                                                                           *
# * Copyright (C) 2003 Cedric Bastoul                                         *
# *                                                                           *
# * This library is free software; you can redistribute it and/or             *
# * modify it under the terms of the GNU Lesser General Public                *
# * License as published by the Free Software Foundation; either              *
# * version 2.1 of the License, or (at your option) any later version.        *
# *                                                                           *
# * This library is distributed in the hope that it will be useful,           *
# * but WITHOUT ANY WARRANTY; without even the implied warranty of            *
# * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU         *
# * Lesser General Public License for more details.                           *
# *                                                                           *
# * You should have received a copy of the GNU Lesser General Public          *
# * License along with this library; if not, write to the Free Software       *
# * Foundation, Inc., 51 Franklin Street, Fifth Floor,                        *
# * Boston, MA  02110-1301  USA                                               *
# *                                                                           *
# * CLooG, the Chunky Loop Generator                                          *
# * Written by Cedric Bastoul, Cedric.Bastoul@inria.fr                        *
# *                                                                           *
# *****************************************************************************/

# This is the main test script of CLooG. It checks that CLooG generates
# a convenient output for an input set of tests, according to some
# parameters (see below). Two checking policies are possible: simply
# compare the generated codes or compare the executions of the generated
# codes. The reference output files must be present: if we are checking a
# file foo.cloog, either foo.c or foo.f must exist in the case of a simple
# code generation checking, and either foo.good.c or foo.good.f must exist
# in the case of a run check.

readonly TEST_NAME="$1"             ## Name of the group of files to test

readonly TEST_FILES="$2"            ## List of test file prefixes and individual
                                    ## options spaces between the elements of
                                    ## one test are represented with '%',
                                    ## e.g., "file -f -1" is "file%-f%-1".

readonly TEST_GLOBAL_OPTIONS="$3"   ## Options for all the tests in the group

readonly TEST_INPUT_EXTENSION="$4"  ## Extension of the input file

readonly TEST_OUTPUT_EXTENSION="$5" ## Extension of the generated file

readonly TEST_TYPE="$6"             ## - "generate" to simply test code
                                    ##   generation (default)
                                    ## - "regenerate" to replace the original
                                    ##   file with the generated one in the
                                    ##   case of a check fail
                                    ##   !!! USE WITH CARE !!!
                                    ## - "valgrind" to test the valgrind output
                                    ##    on code generation
                                    ## - "hybrid" compare source to source and
                                    ##   if this fails run them afterwards

################################################################################
# Global variables
################################################################################

readonly CLOOG_TEMP="cloog_temp_$$"

readonly LOG_DIR="$builddir/logs/$TEST_TYPE-$$"
readonly LOG="$LOG_DIR/checker.log"
readonly SUMMARY="$LOG_DIR/checker.summary.log"

readonly TEXT_NORMAL="\033[0m"
readonly TEXT_BOLD="\033[1m"
readonly TEXT_DIM="\033[2m"
readonly TEXT_UL="\033[4m"
readonly TEXT_INV="\033[7m"

readonly TEXT_BLACK="\033[30m"
readonly TEXT_RED="\033[31m"
readonly TEXT_GREEN="\033[32m"
readonly TEXT_YELLOW="\033[33m"
readonly TEXT_BLUE="\033[34m"
readonly TEXT_PURPLE="\033[35m"
readonly TEXT_CYAN="\033[36m"
readonly TEXT_WHITE="\033[37m"

readonly STEP_GENERATING="generating files"
readonly STEP_REGENERATING="regenerating files"
readonly STEP_COMPILING="compiling files"
readonly STEP_COMPARING="comparing output"
readonly STEP_VALGRIND="running valgrind"
readonly STEP_GENERATING_HYBRID="generating new files (normal test failed)"
readonly STEP_COMPILING_HYBRID="compiling new files (normal test failed)"
readonly STEP_COMPARING_HYBRID="comparing execution (normal test failed)"

readonly TESTLOG_MEMORY_ERROR="Error: memory error or leak detected."
readonly TESTLOG_MEMORY_LEAK="Error: memory leak detected."
readonly TESTLOG_MEMORY_WARNING="Warning: still reachable blocks found..."

################################################################################
# Functions
################################################################################

## log_test(): Log the state of a test
##
## $1:  Destination file (for the logging).
## $2:  Color for the test's state.
## $3:  State of the test.
## $4:  Name of the test.
## $5:  Approximate duration of the test. If irrelevant, pass "".
## $6:  Options of the test. If irrelevant, pass "" or " ".
## $7+: (Optional) Extra information. Each arg is preceded whith a tab and
##      printed on a newline.
log_test ()
{
  dest="$1" color="$2" state="$3" test_name="$4" test_time="$5" details="$6"

  # Erase the current line on standard output in case print_test() has been used
  # previously.
  printf "\r\033[K"
  # Print the state of the test and its options
  printf "$TEXT_BOLD$TEXT_INV$color $state $TEXT_NORMAL" | tee -a "$dest"
  printf "$TEXT_BOLD $test_name $TEXT_NORMAL" | tee -a "$dest"
  if [ -n "$test_time" ]; then
    printf "$TEXT_DIM[$test_time]$TEXT_NORMAL " | tee -a "$dest"
  fi
  if [ -n "$details" -a "$details" != " " ]; then
    printf "$TEXT_DIM(options:$details)$TEXT_NORMAL\n" | tee -a "$dest"
  else
    printf "$TEXT_DIM(no options)$TEXT_NORMAL\n" | tee -a "$dest"
  fi
  # Print extra information, if any.
  if [ $# -gt 6 ]; then
    shift 6
    for arg in "$@"; do
      printf "\t$arg$TEXT_NORMAL\n" | tee -a "$dest"
    done
  fi
}

## log_test_set():  Lo "s"g the state of a test
##
## $1: Destination file (the file which shall contain the states of all tests
##     in the test set and the state of the test set).
## $2: Summary file (the file which shall only contain the state of tests sets).
## $3: Color of the test set's state.
## $4: State of the test set.
## $5: Name of the test set.
## $6: (Optional) Number of tests that passed or failed.
log_test_set ()
{
  log_file="$1" summary_file="$2" color="$3" state="$4" test_set_name="$5"

  printf "\n" | tee -a "$log_file"
  # Print the state of the test set.
  printf "$TEXT_BOLD$TEXT_INV$color %s $TEXT_NORMAL\n" \
         "Test set \"$test_set_name\" $state" \
         | tee -a "$log_file" "$summary_file"
  # Print the number of tests that passed or failed, if specified.
  if [ $# -gt 5 ]; then
    printf "\t$TEXT_DIM%s$TEXT_NORMAL\n" \
           "($6 $([ $6 -gt 1 ] && echo 'tests' || echo 'test') $state)"\
           | tee -a "$log_file" "$summary_file"
  fi
}

## test_done(), test_passed(), test_failed():
##
## Log that a test has been completed, passed or failed. See log_test().
test_done () { log_test "$LOG" "$TEXT_BLUE" "DONE" "$@"; }
test_passed () { log_test "$LOG" "$TEXT_GREEN" "PASS" "$@"; }
test_failed () { log_test "$LOG" "$TEXT_RED" "FAIL" "$@"; }
test_unknown () { log_test "$LOG" "$TEXT_YELLOW" "????" "$@"; }

## pass_test_set(), fail_test_set():
##
## Log that a test set passed or failed. See log_test_set().
pass_test_set () { log_test_set "$LOG" "$SUMMARY" "$TEXT_GREEN" "passed" "$@"; }
fail_test_set () { log_test_set "$LOG" "$SUMMARY" "$TEXT_RED" "failed" "$@"; }

print_step ()
{
  test_name="$1" current_step="$2" log_file="$3"

  step_date=$(date '+%F %H:%M:%S')
  # This line will be erased in normal circumstances. In case of errors,
  # the error's output will most likely be printed without erasing this line.
  # TEXT_RED is added to the end of the line in order to emphasize such errors.
  printf "\r\033[K$TEXT_CYAN$TEXT_INV %s $TEXT_NORMAL$TEXT_CYAN$TEXT_BOLD %s $TEXT_NORMAL$TEXT_CYAN%s$TEXT_RED" \
         "...." "$test_name" "[$step_date]: $current_step..."
  printf "[$step_date]: $current_step\n" >>"$log_file"
}

## print_test_set_name(): Print the name of a test set.
print_test_set_name ()
{
  printf "\n$TEXT_BOLD$TEXT_INV %s $TEXT_NORMAL\n\n" \
         "Testing ClooG: \"$TEST_NAME\" test set" \
         | tee -a "$LOG"
}

## print_test_type(): Print the type of test.
print_test_type ()
{
  printf "$TEXT_DIM$TEXT_INV %s $TEXT_NORMAL\n\n" \
         "Test type: \"$TEST_TYPE\"" \
         | tee -a "$LOG"
}

## fix_env_compile(): Fix environment variable $COMPILE
fix_env_compile ()
{
  # TODO: (nd Cedric) The following line is to deal with the (F*CKING !!!)
  #       space in PACKAGE_STRING, introduced by AC_INIT and which, for
  #       some reason, seems to be the source of a problem with my shell.
  #       Maybe there is a better way to solve the problem...
  COMPILE=$(echo $COMPILE | sed 's/\\\ /_SPACE_/g');
}

## fix_env_link(): Fix environment variable $LINK
fix_env_link ()
{
  # LINK is automatically created by automake which adds "-o $@"...
  # This is fine for most targets, but it may be a problem whith this script.
  LINK="$(echo $LINK | sed 's/-o [^ ]*//') -o $1"
}

## get_seconds(): Get the number of seconds since EPOCH.
##
## stdout: The number of seconds since EPOCH.
get_seconds ()
{
  # Directly using time(1) is (for the time being) too much hassle, the POSIX
  # version of date(1) does not provide the '%s' format...
  # This works in a POSIX-compliant shell.
  PATH=$(getconf PATH) awk 'BEGIN{ srand(); print srand(); }'
}

## get_elapsed_time: Compute the elapsed time.
##
## $1:     Start time in seconds.
## $2:     End time in seconds.
## stdout: The elapsed time in "hoursHH:MM:SS" format.
get_elapsed_time ()
{
  time_in_seconds=$(( $2 - $1 ))
  seconds=$(printf "%02ds" $(( $time_in_seconds % 60 )))
  minutes=$(( ($time_in_seconds / 60) % 60 ))
  hours=$(( ($time_in_seconds / 60) / 60 ))
  if [ $minutes -ne 0 -o $hours -ne 0 ]; then
    minutes=$(printf "%02dmin " "$minutes")
  else
    minutes=""
  fi
  if [ $hours -ne 0 ]; then
    hours=$(printf "%02dh " "$hours")
  else
    hours=""
  fi
  printf "%s%s%s" "$hours" "$minutes" "$seconds"
}

################################################################################
# Body
################################################################################

mkdir -p "$LOG_DIR"
print_test_set_name
print_test_type

failedtest=0;
cloog=$top_builddir/cloog$EXEEXT

for x in $TEST_FILES; do
  name=$(echo $x | sed 's/%/ /g' | cut -d\  -f1);
  individual_options=$(echo $x | sed 's/%/ /g' | cut -s -d\  -f2-);
  input="$srcdir/$name.$TEST_INPUT_EXTENSION";
  output="$srcdir/$name.$TEST_OUTPUT_EXTENSION";
  options="$individual_options $TEST_GLOBAL_OPTIONS";

  input_log="$LOG_DIR/$input.log"
  rm -rf "$input_log"
  mkdir -p $(dirname "$input_log")
  elapsed_time=$(get_seconds)

  if [ "$TEST_TYPE" = "hybrid" ]; then
    # Run CLooG and compare its output to the supposedly correct output.
    print_step "$input" "$STEP_GENERATING" "$input_log"
    $cloog $options -q "$input" -o temp_generated_$$.c

    print_step "$input" "$STEP_COMPILING" "$input_log"
    fix_env_compile
    $COMPILE -P -E temp_generated_$$.c -o temp_generated2_$$.c >/dev/null 2>>$input_log
    $COMPILE -P -E $output -o temp_generated3_$$.c >/dev/null 2>>$input_log

    print_step "$input" "$STEP_COMPARING" "$input_log"
    diff -u -w temp_generated2_$$.c temp_generated3_$$.c >/dev/null 2>>$input_log
    result=$?
    rm temp_generated_$$.c temp_generated2_$$.c temp_generated3_$$.c

    if [ ! $result -eq 0 ]; then
      # If the comparison failed, attempt to run the generated programs and
      # compare the results.
      generate_test=$builddir/generate_test_advanced$EXEEXT
      test_run=$builddir/$$_test_hybrid$EXEEXT
      good="$srcdir/$name.good.$TEST_OUTPUT_EXTENSION";
      if [ $(echo $options | grep -- "-openscop") ]; then
          generate_test="$generate_test -o"
      fi

      print_step "$input" "$STEP_GENERATING_HYBRID" "$input_log"
      $cloog $options -q -callable 1 "$input" -o test_test_$$.c;
      $generate_test "$input" test_main_$$.c >/dev/null 2>>$input_log

      print_step "$input" "$STEP_COMPILING_HYBRID" "$input_log"
      fix_env_compile
      $COMPILE -c test_test_$$.c -o test_test_$$.o >/dev/null 2>>$input_log
      $COMPILE -Dtest=good -c $good -o test_good_$$.o >/dev/null 2>>$input_log
      fix_env_link "$test_run"
      $LINK test_main_$$.c test_test_$$.o test_good_$$.o >/dev/null 2>>$input_log

      print_step "$input" "$STEP_COMPARING_HYBRID" "$input_log"
      $test_run;
      result=$?;

      elapsed_time=$(get_elapsed_time $elapsed_time $(get_seconds))
      if [ $result -eq 0 ]; then
        test_passed "$input" "$elapsed_time" "$options" \
          "$TEXT_YELLOW""Output comparison failed, execution comparison passed."
      elif [ $result -eq 1 ]; then
        test_unknown "$input" "$elapsed_time" "$options" \
          "$TEXT_YELLOW""The test did not compute anything => Validity unknown."
      else
        test_failed "$input" "$elapsed_time" "$options"
      fi

      rm -f $test_run test_main_$$.c test_test_$$.o test_test_$$.c test_good_$$.o;
    else
      elapsed_time=$(get_elapsed_time $elapsed_time $(get_seconds))
      test_passed "$input" "$elapsed_time" "$options"
    fi

  elif [ "$TEST_TYPE" = "valgrind" ]; then
    print_step "$input" "$STEP_VALGRIND" "$input_log"
    libtool --mode=execute valgrind --error-exitcode=1 --leak-check=full \
            $cloog $options -q "$input" >/dev/null 2>>$CLOOG_TEMP
    errors=$?;

    leaks=$(grep "in use at exit:" $CLOOG_TEMP | cut -f 2 -d ':')
    reachable=$(tail $CLOOG_TEMP | grep "still reachable:" | cut -f 2 -d ':')

    elapsed_time=$(get_elapsed_time $elapsed_time $(get_seconds))
    if [ "$errors" = "1" ]; then
      # Some memory errors or memory leaks (depending on the options passed to
      # valgrind, memory leaks may or may not be counted as errors...) have been
      # detected.
      result="1";
      test_failed "$input" "$elapsed_time" "$options" \
                  "$TEXT_RED""$TESTLOG_MEMORY_ERROR" \
                  "$TEXT_DIM""Check $TEXT_UL$input_log$TEXT_NORMAL$TEXT_DIM." \
                  "$TEXT_DIM""Last 15 lines of $TEXT_UL$input_log:" \
                  "$TEXT_DIM""$(tail -n 15 $input_log)"
    elif [ "$errors" != "0" ]; then
      # The tested program crashed.
      result="1";
      test_failed "$input" "$elapsed_time" "$options" \
                  "$TEXT_RED""Exit code: $errors"
    elif [ "$leaks" != " 0 bytes in 0 blocks" ]; then
      if [ "$leaks" = "$reachable" ]; then
        # Some still reachable memory has been detected.
        # This is **probably** OK. The test passes, with a warning.
        result="0";
        test_passed "$input" "$elapsed_time" "$options" \
                    "$TEXT_YELLOW""$TESTLOG_MEMORY_WARNING" \
                    "$TEXT_DIM(See $TEXT_UL$input_log$TEXT_NORMAL$TEXT_DIM.)"
      else
        # Memory leaks have been detected.
        result="1";
        test_failed "$input" "$elapsed_time" "$options" \
                    "$TEXT_RED""$TESTLOG_MEMORY_LEAK" \
                    "$TEXT_DIM""Check $TEXT_UL$input_log." \
                    "$TEXT_DIM""Last 15 lines of $TEXT_UL$input_log:" \
                    "$TEXT_DIM""$(tail -n 15 $input_log)"
      fi
    else
      result="0";
      test_passed "$input" "$elapsed_time" "$options"
    fi;

    cat $CLOOG_TEMP >> "$input_log"
    rm -f $CLOOG_TEMP

  else
    # Test the file generation.
    print_step "$input" "$STEP_GENERATING" "$input_log"
    $cloog $options -q "$input" -o $CLOOG_TEMP 2>> "$input_log"
    diff -u -w --ignore-matching-lines='CLooG' $output $CLOOG_TEMP;
    result=$?;
    if [ "$result" -ne "0" ] && [ "$TEST_TYPE" = "regenerate" ]; then
      print_step "$input" "$STEP_REGENERATING" "$input_log"
      cp $CLOOG_TEMP $output;
    fi;
    elapsed_time=$(get_elapsed_time $elapsed_time $(get_seconds))
    test_done "$input" "$elapsed_time" "$options"
    rm -f $CLOOG_TEMP;
  fi;

  if [ "$result" -ne "0" ]; then
    failedtest=$(( $failedtest + 1 ));
  fi
done;

if [ $failedtest != 0 ]; then
  fail_test_set "$TEST_NAME" "$failedtest"
else
  pass_test_set "$TEST_NAME"
fi
exit $failedtest
