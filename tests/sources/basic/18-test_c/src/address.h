#ifndef ADDRESS_h
#define ADDRESS_H

#include <string>
#include <boost/serialization/string.hpp>
#include <boost/serialization/serialization.hpp>
#include <boost/serialization/access.hpp>

using namespace std;
using namespace boost;
using namespace serialization;

class address {

public:
	string name;
	int number;
	int flat;
	char letter;
	void print();
	address(){};

private:
	friend class::serialization::access;
	template<class Archive>
	void serialize(Archive & ar, const unsigned int version) {
		ar & name;
		ar & number;
		ar & flat;
		ar & letter;
	}
};

#endif
