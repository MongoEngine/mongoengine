import sys
sys.path[0:0] = [""]
import unittest

from .class_methods import *
from .delta import *
from .dynamic import *
from .indexes import *
from .inheritance import *
from .instance import *
from .json_serialisation import *
from .validation import *

if __name__ == '__main__':
    unittest.main()
