OpenScop - Parallel Python Translator                                                                                                                                                                                                                                        
=============================

Uses the [PLUTO][pluto] to generate a Parallel Python code from an input
[OpenScop][openscop] file. Notice that the input file must be in a valid OpenScop
format and that it must fulfill the PLUTO restrictions in order to be automatically
parallelized.

The generated file is written in Python and annotated with comments in a OMP similar
fashion (annotations are of the form `# parallel for PRIVATE(lbv,ubv,t3) REDUCTION()`).
However, the obtained code cannot be directly executed in parallel since annotations
must be processed by some Runtime. 


### Module Dependencies

- A valid [PLUTO][pluto] installation
- Uses the [subprocess][subprocess] Python module
- [Logging][logging] Python module
- [UnitTest][unittest] Python module

### Extra Dependencies

- To run all tests you require the [Nose][nose] Python module
- To add code coverage you require [coverage][coverage] and/or 
[codacy-coverage][codacy] Python modules


### Test with debug

```
python translator_scop2pscop2py.py
```


### Test without debug

```
python -O translator_scop2pscop2py.py
```


### Run

```
import Scop2PScop2Py
Scop2PScop2Py.translate(source_file, output_file)
```


### Clean

```
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete
```


[pluto]: https://github.com/periscop/openscop
[openscop]: https://github.com/periscop/openscop
[subprocess]: https://docs.python.org/2/library/subprocess.html
[logging]: https://docs.python.org/2/library/logging.html
[unittest]: https://docs.python.org/2/library/unittest.html
[nose]: https://nose.readthedocs.io/en/latest/
[coverage]: https://coverage.readthedocs.io/en/coverage-4.4.2/
[codacy]: https://github.com/codacy/python-codacy-coverage

