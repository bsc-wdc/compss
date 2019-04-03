#include<iostream>
using namespace std;

/*
 * I am assuming that a byte is 8 bits... 
 * I don't know when is not but seems like sometimes IS NOT
 */

void split_long(long a, int* pair) {
    #pragma oss task 
    {
        pair[0] = (int)a;

        pair[1] = a >> (sizeof(int)*8);
    }
}

void split_int(int N, int M, int* a, short* pair) {
    
    for (int i = 0; i < N; ++i) {
        #pragma oss task
        {
        int j=i<<1;

        pair[j]     = (short)a[i];
        pair[j+1]   = a[i] >> (sizeof(short)*8);
        }
    }

}

void split_short(int N, int M, short* a, char* pair) {

    for(int i = 0; i < N; ++i) {
        #pragma oss task
        {
        int j=i<<1;

        pair[j]     = (char)a[i];
        pair[j+1]   = a[i] >> (sizeof(char)*8);
        }
    }

}

void make_string(char a, char b, char c, char* str) {
    
    cout << "[JOB] Executing make function..." << endl;

    #pragma oss task out(str)
    {
        str[0] = a;
    }

    #pragma oss task inout(str)
    {
        str[1] = b;
    }

    #pragma oss task inout(str)
    {
        str[2] = c;
    }

    #pragma oss task inout(str)
    {
        str[3] = '\0';
    }

    cout << "[JOB] Generated string" << str << endl;

}

//Fixed length of 3 in the array...
long* make_long_array(long a, long b, long c) {

    long* arr = new long[3];
    
    #pragma oss task
    {
        arr[0] = a;
        arr[1] = b;
        arr[2] = c;
    }

    return arr;

}

void sum_long_array(long* array) {
    array[0] = array[0] + array[1] + array[2];
}

int* make_int_array(int a, int b, int c) {
    return NULL;
}

void sum_int_array(int* array) {

}

short* make_short_array(short a, short b, short c) {
    return NULL;
}

void sum_short_array(short* array) {

}

void split_string(char* str) {

    cout << "[JOB] Executing split function..." << endl;

    char a, b, c;

    #pragma oss task
    {
        a = str[0];
        b = str[1];
        c = str[2];
    }

    cout << "[JOB] a: " << a << " b: " << b << " c: " << c << endl;

}
