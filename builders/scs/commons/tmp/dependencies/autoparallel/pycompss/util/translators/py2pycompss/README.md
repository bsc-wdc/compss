Python - PyCOMPSs Translator
=============================

Constructs a [PyCOMPSs][pycompss] code by applying the suggested parallelizations to 
the original function. The original function must by a Python function and the 
suggested parallelizations must be written in Python and annotated in a OMP similar
fashion (see [OpenScop to Python Translator][scop2pscop2py] translator). 


### Module Dependencies

- [AST][ast] Python module
- [AST Observe/Rewrite (ASTOR)][astor] Python module
- [SymPy][sympy] Python module
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
import Py2PyCOMPSs

func = <python_function_object>
par_py_files = [<par_py1>, <par_py2>, ...]
out_file = <pycompss_output_file>

Py2PyCOMPSs.translate(func, par_py_files, out_file)
```


### Clean

```
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete
```

[pycompss]: http://compss.bsc.es
[scop2pscop2py]: https://github.com/cristianrcv/pycompss-pluto/tree/master/pycompss/util/translators/scop2pscop2py
[ast]: https://docs.python.org/2/library/ast.html
[astor]: http://astor.readthedocs.io/en/latest/
[sympy]: http://www.sympy.org/es/
[logging]: https://docs.python.org/2/library/logging.html
[unittest]: https://docs.python.org/2/library/unittest.html
[nose]: https://nose.readthedocs.io/en/latest/
[coverage]: https://coverage.readthedocs.io/en/coverage-4.4.2/
[codacy]: https://github.com/codacy/python-codacy-coverage
