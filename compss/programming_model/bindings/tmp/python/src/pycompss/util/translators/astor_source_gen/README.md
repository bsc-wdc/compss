ASTOR Source Generator for PyCOMPSs
=============================

Source Generator re-implementation for PyCOMPSs (with comments and more line length)


### Module Dependencies

- [AST Observe/Rewrite (ASTOR)][astor] Python module
- [UnitTest][unittest] Python module


### Extra Dependencies

- To run all tests you require the [Nose][nose] Python module
- To add code coverage you require [coverage][coverage] and/or
[codacy-coverage][codacy] Python modules


### Test with debug

```
python pycompss_source_gen.py
```


### Test without debug

```
python -O pycompss_source_gen.py
```


### Run

```
main_node.body[0].insert(0, BlockComment("This is a block comment"))
print(PyCOMPSsSourceGen.to_source(main_node))
```


### Clean

```
find . -name "*.pyc" -delete
find . -name "*.pyo" -delete
```

[astor]: http://astor.readthedocs.io/en/latest/
[unittest]: https://docs.python.org/2/library/unittest.html
[nose]: https://nose.readthedocs.io/en/latest/
[coverage]: https://coverage.readthedocs.io/en/coverage-4.4.2/
[codacy]: https://github.com/codacy/python-codacy-coverage
