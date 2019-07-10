#include <iostream>
#include <c_compss.h>
#include "type_tests.h"

using namespace std;

/*  COMPSs C-Binding test : Type_tests
 *
 *  This test is highly related with the architecture of the machine.
 *  It has been designed for a 64 bit processor with byte order Little Endian.
 *  We also take for granted that the size of a byte is 8 bits.
 */

int main() {

    cout << "Encendemos el runtime... " << endl;
    compss_on();

    //Test with long, int, short and char
    //----------------------------------------------------------------------

    char initial = '@';
    long _long = 0;

    for (long i = 1; i <= sizeof(long); ++i) {
   
        if (i == sizeof(long)) {
            _long = _long | (initial + i);
        }
        else {
            _long = _long | ( (initial + i) << ( ( (sizeof(long) - i)*8) ) );
        }
    }

    // Note that _long can be intepreted as an array of chars which is ABCDEFGH     

    int* int_pair = (int*)malloc(2 * sizeof(int));

    split_long(_long, int_pair);

    //Splitted the long in two ints, now we can split the same two ints into
    //four shorts, so we will have to make two calls two split_short...

    short* short_array = (short*)malloc(4 * sizeof(short));

    split_int(2, 2*2, int_pair, short_array);

    //Splitted the two ints into four shorts, now we will do the same
    //splitting four shorts into eight chars.

    char* char_array = (char*)malloc(8 * sizeof(char));

    split_short(4, 2*4, short_array, char_array);

    //Now we are going to test whether the splits we made were correct

    compss_wait_on(int_pair);

    if (not (((int)(_long >> (sizeof(int)*8)) == int_pair[1]) and (((int)_long) == int_pair[0]))) {
        cout << "The 'int' splitted values does not match with the original value." << endl;
        return -1;
    }
    else {
        cout << "split_long... works" << endl;
    }
    
    compss_wait_on(short_array);

    if (not ((short)(int_pair[1]    >> (sizeof(short)*8)) == short_array[3] and
             (short)(int_pair[1])   == short_array[2]                       and
             (short)(int_pair[0]    >> (sizeof(short)*8)) == short_array[1] and
             (short)(int_pair[0])   == short_array[0])) {

        cout << "The 'short' splitted values does not match with the original values" << endl;
        return -1;

    }
    else {
        cout << "split_short... works" << endl;
    }
    
    compss_wait_on(char_array);

    if (not (((char)(short_array[3]  >> (sizeof(char)*8)) == char_array[7])     and
             ((((char)short_array[3]) == char_array[6]))                        and
             (((char)(short_array[2]  >> (sizeof(char)*8))) == char_array[5])   and
             ((((char)short_array[2]) == char_array[4]))                        and
             (((char)(short_array[1]  >> (sizeof(char)*8))) == char_array[3])   and
             ((((char)short_array[1]) == char_array[2]))                        and
             (((char)(short_array[0]  >> (sizeof(char)*8))) == char_array[1])   and
             ((((char)short_array[0]) == char_array[0])))) {

        cout << "The 'char' splitted value does not match with the original value." << endl;
        return -1;
    }
    else {
        cout << "split_char... works" << endl;
    }

    //----------------------------------------------------------------------

    //Test with long and long*
    //----------------------------------------------------------------------
    
    long long_a, long_b, long_c;
    long_a = long_b = long_c = 1;

    long long_result = long_a + long_b + long_c;

    long* long_array;
    long_array = make_long_array(long_a, long_b, long_c);

    sum_long_array(long_array);

    compss_wait_on(long_array);

    if (long_array[0] != long_result) {
       cout << "The expected result when executing task sum_long_array is: " << long_result <<
               " and we got: " << long_array[0] << endl;
        return -1; 
    }
    else {
        cout << "make_long_array... works" << endl << "sum_long_array... works" << endl;
    }

    //----------------------------------------------------------------------

    //Test with int and int*
    //----------------------------------------------------------------------
    
    int int_a, int_b, int_c;
    int_a = int_b = int_c = 1;

    int int_result = int_a + int_b + int_c;

    int* int_array;
    int_array = make_int_array(int_a, int_b, int_c);

    sum_int_array(int_array);
   
    compss_wait_on(int_array);
 
    if (int_array[0] != int_result) {
       cout << "The expected result when executing task sum_int_array is: " << int_result <<
               " and we got: " << int_array[0] << endl;
        return -1; 
    }
    else {
        cout << "make_int_array... works" << endl << "sum_int_array... works" << endl;
    }
    
    //----------------------------------------------------------------------

    //Test with short and short*
    //----------------------------------------------------------------------

    short short_a, short_b, short_c;
    short_a = short_b = short_c = 1;

    short short_result = short_a + short_b + short_c;

    short_array = make_short_array(short_a, short_b, short_c);

    sum_short_array(short_array);
   
    compss_wait_on(short_array);
 
    if (short_array[0] != short_result) {
       cout << "The expected result when executing task sum_short_array is: " << short_result <<
               " and we got: " << short_array[0] << endl;
        return -1; 
    }
    else {
        cout << "make_short_array... works" << endl << "sum_short_array... works" << endl;
    }

    //----------------------------------------------------------------------

    //Test with float and float*
    //----------------------------------------------------------------------

    float float_a, float_b, float_c;
    float_a = float_b = float_c = 3.33;

    float float_result = float_a + float_b + float_c;

    float* float_array;
    float_array = make_float_array(float_a, float_b, float_c);

    sum_float_array(float_array);
   
    compss_wait_on(float_array);
 
    if (not (float_array[0] <= (float_result+0.1) and float_array[0] >= (float_result-0.1))) {
       cout << "The expected result when executing task sum_float_array is between: " << float_result+0.1 << " " 
            << float_result-0.1 << " and we got: " << float_array[0] << endl;
        return -1; 
    }
    else {
        cout << "make_float_array... works" << endl << "sum_float_array... works" << endl;
    }

    //----------------------------------------------------------------------

    //Test with double and double*
    //----------------------------------------------------------------------

    double double_a, double_b, double_c;
    double_a = double_b = double_c = 3.33;

    double double_result = double_a + double_b + double_c;

    double* double_array;
    double_array = make_double_array(double_a, double_b, double_c);

    sum_double_array(double_array);
   
    compss_wait_on(double_array);
 
    if (not (double_array[0] <= double_result+0.1 and double_array[0] >= double_result-0.1)) {
       cout << "The expected result when executing task sum_double_array is between: " << double_result+0.1 << " " 
            << double_result-0.1 << " and we got: " << double_array[0] << endl;
        return -1; 
    }
    else {
        cout << "make_double_array... works" << endl << "sum_double_array... works" << endl;
    }

    //----------------------------------------------------------------------

    //Test with char and char*
    //----------------------------------------------------------------------

    char a = 'L';
    char b = 'K';
    char c = 'M';

    char* str;
    
    make_string(a, b, c, str);

    split_string(str); //This one is useless, it only tests inside, but returns nothing!

    compss_wait_on(str);

    string string_str(str);

    if (string_str != "LKM") {
       cout << "The expected result when executing make_string is : \"LKM\" and we got: " << string_str << endl;
        return -1;
    }
    else {
        cout << "make_string... works" << endl;
    }
    
    //----------------------------------------------------------------------

    cout << "Apagamos el runtime... "   << endl;
    compss_off();

}
