#include "student.h"
#include <iostream>


void student::print(){
	cout << "student:"<< name <<" "<< surname <<" ("<<age << ")"<< endl; 
	domicile.print();
}
