from context import fpop
import os,sys,json,glob,shutil,textwrap
import dpdata
import numpy as np
import unittest
from fpop.cp2k import Cp2kInputs
from pathlib import Path

class TestCP2KInputs(unittest.TestCase):
    def setUp(self):
        Path('template.inp').write_text('&GLOBAL\n  PROJECT foo\n&END GLOBAL\n')

    def tearDown(self):
        os.remove('template.inp')

    def test_cp2k_input(self):
        iinp_file = 'template.inp'
        ci = Cp2kInputs(iinp_file)
        self.assertEqual(ci.inp_template, '&GLOBAL\n  PROJECT foo\n&END GLOBAL\n')
