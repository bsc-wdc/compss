#include <iostream>
#include <c_compss.h>
#include "type_tests.h"

using namespace std;

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
    cout << dec << _long << endl; 

    // Note that _long can be intepreted as an array of chars which is ABCDEFGH     

    int* int_pair = (int*)malloc(2 * sizeof(int));

    split_long(_long, int_pair);

    //Splitted the long in two ints, now we can split the same two ints into
    //four shorts, so we will have to make two calls two split_short...

    short* short_array = (short*)malloc(4 * sizeof(short));

    split_int(2, 2*2, int_pair, short_array);

    compss_wait_on(short_array);

    for (int i = 0; i < 4; ++i) {
        cout << short_array[i] << endl;
    }

    if (not ((short)(int_pair[1]    >> (sizeof(short)*8)) == short_array[3] and
             (short)(int_pair[1])   == short_array[2]                       and
             (short)(int_pair[0]    >> (sizeof(short)*8)) == short_array[2] and
             (short)(int_pair[0])   == short_array[0])) {

        cout << "The 'short' splitted values does not match with the original values" << endl;
        return -1;

    }

    return 0; //TODO exit here now..

    //Splitted the two ints into four shorts, now we will do the same
    //splitting four shorts into eight chars.

    char* char_array = (char*)malloc(8 * sizeof(char));

    split_short(4, 2*4, short_array, char_array);

    //Now we are going to test whether the splits we made were correct

    compss_wait_on(int_pair);

    cout << "leftmost element : " << int_pair[1] << " rightmost element : " << int_pair[0] << endl;

    if (not (((int)(_long >> (sizeof(int)*8)) == int_pair[1]) and (((int)_long) == int_pair[0]))) {
        cout << "The splitted values does not match the original value." << endl;
        return -1;
    }
    else {
        cout << "split_long ... works" << endl;
    }

    return 0;

    //----------------------------------------------------------------------



    //Test with char and char*
    //----------------------------------------------------------------------

    char a = 'L';
    char b = 'K';
    char c = 'M';

    char* str;
    
    make_string(a, b, c, str);

    split_string(str);

    compss_wait_on(str);

    cout << "str: " << str  << endl;
    
    //----------------------------------------------------------------------

    cout << "Apagamos el runtime... "   << endl;
    compss_off();

}
