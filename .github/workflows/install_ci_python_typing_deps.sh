#!/bin/bash
pip install --upgrade pip
pip install mypy==1.10.1 typing-extensions mongomock types-Pygments types-cffi types-colorama types-pyOpenSSL types-python-dateutil types-requests types-setuptools
pip install -e '.[test]'
