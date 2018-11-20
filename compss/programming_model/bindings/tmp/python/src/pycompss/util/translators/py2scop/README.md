Python - OpenScop Translator
=============================

Processes a Python function and writes the [OpenScop][openscop] representation of
all the main loops found. 


### Module Dependencies

- [Inspect][inspect] Python module
- [AST][ast] Python module
- [AST Observe/Rewrite (ASTOR)][astor] Python module
- [Logging][logging] Python module
- [UnitTest][unittest] Python module


### Extra Dependencies

- To run all tests you require the [Nose][nose] Python module
- To add code coverage you require [coverage][coverage] and/or
[codacy-coverage][codacy] Python modules


### Test with debug

```
python translator_py2scop.py
```


### Test without debug

```
python -O translator_py2scop.py
```


### Run

```
import Py2Scop

func = <Python_function_object>
base_output = <base_scop_output_file>

translator = Py2Scop(func)
output_files = translator.translate(base_output)
```


### Clean

```
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete
```


[inspect]: https://docs.python.org/2/library/inspect.html
[ast]: https://docs.python.org/2/library/ast.html
[astor]: http://astor.readthedocs.io/en/latest/
[openscop]: https://github.com/periscop/openscop
[logging]: https://docs.python.org/2/library/logging.html
[unittest]: https://docs.python.org/2/library/unittest.html
[nose]: https://nose.readthedocs.io/en/latest/
[coverage]: https://coverage.readthedocs.io/en/coverage-4.4.2/
[codacy]: https://github.com/codacy/python-codacy-coverage
