from __future__ import absolute_import
import sys
sys.path[0:0] = [""]
import unittest

from tests.document.class_methods import *
from tests.document.delta import *
from tests.document.dynamic import *
from tests.document.indexes import *
from tests.document.inheritance import *
from tests.document.instance import *
from tests.document.json_serialisation import *
from tests.document.validation import *

if __name__ == '__main__':
    unittest.main()
