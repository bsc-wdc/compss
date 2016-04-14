#ifndef STUDENT_H
#define STUDENT_H

#include <string>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/access.hpp>

#include "address.h"

using namespace std;
using namespace boost;
using namespace serialization;

class student {

public:
	string name;
	string surname;
	int age;
	address domicile;

	student(){};

private:
	friend class::serialization::access;
	template<class Archive>
	void serialize(Archive & ar, const unsigned int version) {
		ar & name;
		ar & surname;
		ar & age;
		ar & domicile;
	}
};

#endif
