#ifndef ADDRESS_h
#define ADDRESS_H

#include <string>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
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
