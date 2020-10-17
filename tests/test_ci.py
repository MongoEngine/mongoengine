def test_ci_placeholder():
    # This empty test is used within the CI to
    # setup the tox venv without running the test suite
    # if we simply skip all test with pytest -k=wrong_pattern
    # pytest command would return with exit_code=5 (i.e "no tests run")
    # making travis fail
    # this empty test is the recommended way to handle this
    # as described in https://github.com/pytest-dev/pytest/issues/2393
    pass
