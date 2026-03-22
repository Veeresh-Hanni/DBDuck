# Contributing

1. Create a virtual environment.
2. Install development dependencies with `pip install -e .[dev,all]`.
3. Run `pytest -q` and `python -m bandit -q -r DBDuck` before opening a pull request.
4. Keep public UDOM APIs backward compatible.
5. Add or update tests for every behavioral change.

