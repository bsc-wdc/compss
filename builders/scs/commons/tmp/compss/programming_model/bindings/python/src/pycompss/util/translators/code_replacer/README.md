Code Replacer
=============================

Replaces the code of a given function by the code contained in the new_file and
loads the new function code. The translator backups the original function 
(`<original>_bkp.py`) and stores the new code (`<original>_autogen.py`) in separated
files that can be kept or removed using the `keep_generated_files` flag.  


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
python code_replacer.py
```


### Test without debug

```
python -O code_replacer.py
```


### Run

```
import CodeReplacer
func = <func instance>
new_code = <path_to_file_containing_new_code>

cr = CodeReplacer(func)
new_func = cr.replace(new_code, keep_generated_files=False)

cr.clean()
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
