from setuptools import setup

VERSION = '0.1.1'

DESCRIPTION = "A Python Document-Object Mapper for working with MongoDB"

LONG_DESCRIPTION = None
try:
    LONG_DESCRIPTION = open('README.rst').read()
except:
    pass

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Database',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(name='mongoengine',
      version=VERSION,
      packages=['mongoengine'],
      author='Harry Marr',
      author_email='harry.marr@{nospam}gmail.com',
      url='http://hmarr.com/mongoengine/',
      license='MIT',
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      platforms=['any'],
      classifiers=CLASSIFIERS,
      install_requires=['pymongo'],
      test_suite='tests',
)
