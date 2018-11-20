Argument Utils
=============================

Auxiliar functions and classes to flatten and rebuild function arguments.


### Module Dependencies

- [Logging][logging] Python module
- [UnitTest][unittest] Python module


### Extra Dependencies

- To run all tests you require the [Nose][nose] Python module
- To add code coverage you require [coverage][coverage] and/or
[codacy-coverage][codacy] Python modules


### Test with debug

```
python arg_utils.py
```


### Test without debug

```
python -O arg_utils.py
```


### Run

```
import random
size1 = 10
size2 = 5
l1 = [random.random() for _ in range(size1)]
l2 = [[random.random() for _ in range(size2)] for _ in range(size2)]

import ArgUtils
flat_args = ArgUtils.flatten_args(l1, l2)

built_args = ArgUtils.rebuild_args(flat_args)
```


### Clean

```
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete
```

[logging]: https://docs.python.org/2/library/logging.html
[unittest]: https://docs.python.org/2/library/unittest.html
[nose]: https://nose.readthedocs.io/en/latest/
[coverage]: https://coverage.readthedocs.io/en/coverage-4.4.2/
[codacy]: https://github.com/codacy/python-codacy-coverage
