import os
import sys

from pkg_resources import normalize_path
from setuptools import find_packages, setup
from setuptools.command.test import test as TestCommand

# Hack to silence atexit traceback in newer python versions
try:
    import multiprocessing  # noqa: F401
except ImportError:
    pass

DESCRIPTION = "MongoEngine is a Python Object-Document Mapper for working with MongoDB."

try:
    with open("README.rst") as fin:
        LONG_DESCRIPTION = fin.read()
except Exception:
    LONG_DESCRIPTION = None


def get_version(version_tuple):
    """Return the version tuple as a string, e.g. for (0, 10, 7),
    return '0.10.7'.
    """
    return ".".join(map(str, version_tuple))


class PyTest(TestCommand):
    """Will force pytest to search for tests inside the build directory
    for 2to3 converted code (used by tox), instead of the current directory.
    Required as long as we need 2to3

    Known Limitation: https://tox.readthedocs.io/en/latest/example/pytest.html#known-issues-and-limitations
    Source: https://www.hackzine.org/python-testing-with-pytest-and-2to3-plus-tox-and-travis-ci.html
    """

    # https://pytest.readthedocs.io/en/2.7.3/goodpractises.html#integration-with-setuptools-test-commands
    # Allows to provide pytest command argument through the test runner command `python setup.py test`
    # e.g: `python setup.py test -a "-k=test"`
    # This only works for 1 argument though
    user_options = [("pytest-args=", "a", "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ""

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ["tests"]
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        from pkg_resources import _namespace_packages

        # Purge modules under test from sys.modules. The test loader will
        # re-import them from the build location. Required when 2to3 is used
        # with namespace packages.
        if sys.version_info >= (3,) and getattr(self.distribution, "use_2to3", False):
            module = self.test_args[-1].split(".")[0]
            if module in _namespace_packages:
                del_modules = []
                if module in sys.modules:
                    del_modules.append(module)
                module += "."
                for name in sys.modules:
                    if name.startswith(module):
                        del_modules.append(name)
                map(sys.modules.__delitem__, del_modules)

            # Run on the build directory for 2to3-built code
            # This will prevent the old 2.x code from being found
            # by py.test discovery mechanism, that apparently
            # ignores sys.path..
            ei_cmd = self.get_finalized_command("egg_info")
            self.test_args = [normalize_path(ei_cmd.egg_base)]

        cmd_args = self.test_args + ([self.pytest_args] if self.pytest_args else [])
        errno = pytest.main(cmd_args)
        sys.exit(errno)


# Dirty hack to get version number from monogengine/__init__.py - we can't
# import it as it depends on PyMongo and PyMongo isn't installed until this
# file is read
init = os.path.join(os.path.dirname(__file__), "mongoengine", "__init__.py")
version_line = list(filter(lambda line: line.startswith("VERSION"), open(init)))[0]

VERSION = get_version(eval(version_line.split("=")[-1]))

CLASSIFIERS = [
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Python :: Implementation :: PyPy",
    "Topic :: Database",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

extra_opts = {
    "packages": find_packages(exclude=["tests", "tests.*"]),
    "tests_require": [
        "pytest",
        "pytest-cov",
        "coverage",
        "blinker",
        "Pillow>=7.0.0",
    ],
}

if "test" in sys.argv:
    extra_opts["packages"] = find_packages()
    extra_opts["package_data"] = {
        "tests": ["fields/mongoengine.png", "fields/mongodb_leaf.png"]
    }

setup(
    name="mongoengine",
    version=VERSION,
    author="Harry Marr",
    author_email="harry.marr@gmail.com",
    maintainer="Stefan Wojcik",
    maintainer_email="wojcikstefan@gmail.com",
    url="http://mongoengine.org/",
    download_url="https://github.com/MongoEngine/mongoengine/tarball/master",
    license="MIT",
    include_package_data=True,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    platforms=["any"],
    classifiers=CLASSIFIERS,
    python_requires=">=3.7",
    install_requires=["pymongo>=3.4,<5.0"],
    cmdclass={"test": PyTest},
    **extra_opts
)
