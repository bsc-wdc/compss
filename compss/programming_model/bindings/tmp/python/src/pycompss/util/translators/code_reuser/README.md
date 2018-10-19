Code Reuser
=============================

Reuses a parallelized code of the given function that was previously stored on the `<original>_autogen.py` file.
The original code is stored in a backup file (`<original>_bkp.py`).


### Module Dependencies

- [Inspect][inspect] Python module
- [Logging][logging] Python module
- [UnitTest][unittest] Python module


### Extra Dependencies

- To run all tests you require the [Nose][nose] Python module
- To add code coverage you require [coverage][coverage] and/or
[codacy-coverage][codacy] Python modules


### Test with debug

```
python code_reuser.py
```


### Test without debug

```
python -O code_reuser.py
```


### Run

```
import CodeReuser
func = <func instance>

cr = CodeReuser(func, force_autogen=False)
new_func = cr.reuse()

cr.restore()
```


### Clean

```
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete
```

[inspect]: https://docs.python.org/2/library/inspect.html
[logging]: https://docs.python.org/2/library/logging.html
[unittest]: https://docs.python.org/2/library/unittest.html
[nose]: https://nose.readthedocs.io/en/latest/
[coverage]: https://coverage.readthedocs.io/en/coverage-4.4.2/
[codacy]: https://github.com/codacy/python-codacy-coverage
