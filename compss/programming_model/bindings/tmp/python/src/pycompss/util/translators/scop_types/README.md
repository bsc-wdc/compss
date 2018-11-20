Python OpenScop Representation
=============================

A Python Object representation of the [OpenScop][openscop] format. It is based on the 
OpenScop documentation and allows users read and write Python objects
from/to an OpenScop file.


### Module Dependencies

- [UnitTest][unittest] Python module


### Extra Dependencies

- To run all tests you require the [Nose][nose] Python module
- To add code coverage you require [coverage][coverage] and/or 
[codacy-coverage][codacy] Python modules


### Test with debug

```
python scop_class.py
```


### Test without debug

```
python -O scop_class.py
```


### Run

```
import scop

# Constructor
s = Scop(global, statements, extensions)

# Read from OpenScop file
s = Scop.read_os('file.scop')

# Write to file
with open('file.scop', 'w') as f:
    s.write_os(f)
```


### Clean

```
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete
```


[openscop]: https://github.com/periscop/openscop
[unittest]: https://docs.python.org/2/library/unittest.html
[nose]: https://nose.readthedocs.io/en/latest/
[coverage]: https://coverage.readthedocs.io/en/coverage-4.4.2/
[codacy]: https://github.com/codacy/python-codacy-coverage
