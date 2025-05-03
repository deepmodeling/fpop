import os
import unittest
from dflow.python import (
    OPIO,
)

import shutil
from pathlib import Path
from fpop.abacus import PrepAbacus,AbacusInputs


class TestPrepAbacus(unittest.TestCase):
    '''
    deepmd/npy format named ["data.000","data.001"].
    no optional_input or optional_artifact.
    '''
    def setUp(self):
        # config in spin/deepmd-spin is contants.POSCAR_1_content, and add spins = [[[1,2,3]]]
        # here to test if prep_fp can read the spins data.
        self.confs = [Path("spin/deepmd-spin")]
        self.ntasks = len(self.confs)
        self.type_map = ['Na']

        self.source_path = Path('abacustest')
        self.source_path.mkdir(parents=True, exist_ok=True)
        (self.source_path/"INPUT").write_text('INPUT_PARAMETERS\ncalculation scf\nbasis_type lcao\n')
        (self.source_path/"KPT").write_text('here kpt')
        (self.source_path/"Na.upf").write_text('here upf')
        (self.source_path/"Na.orb").write_text('here orb')
        (self.source_path/'optional_test').write_text('here test')

        self.abacus_inputs = AbacusInputs(
            input_file=self.source_path/"INPUT",
            kpt_file=self.source_path/"KPT",
            pp_files={"Na":self.source_path/"Na.upf"},
            orb_files={"Na":self.source_path/"Na.orb"}
        )
    
    def tearDown(self):
        for ii in range(self.ntasks):
            work_path = Path("task.%06d"%ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)
        if os.path.isdir(self.source_path):
            shutil.rmtree(self.source_path)

    def checkfile(self):
        tdir = "task.000000"
        print(tdir)
        self.assertTrue(Path(tdir).is_dir())
        self.assertTrue(os.path.isfile(Path(tdir)/'INPUT'))
        self.assertTrue(os.path.isfile(Path(tdir)/'KPT'))
        self.assertTrue(os.path.isfile(Path(tdir)/'STRU'))
        self.assertTrue(os.path.isfile(Path(tdir)/'Na.upf'))
        self.assertTrue(os.path.isfile(Path(tdir)/'Na.orb'))
        self.assertEqual((Path(tdir)/'INPUT').read_text().split()[0],"INPUT_PARAMETERS")      
        self.assertEqual((Path(tdir)/'KPT').read_text(),'here kpt')
        self.assertEqual((Path(tdir)/'Na.upf').read_text(),'here upf')
        self.assertEqual((Path(tdir)/'Na.orb').read_text(),'here orb')
        stru_c = (Path(tdir)/'STRU').read_text()
        self.assertTrue("0.000000000000 0.000000000000 0.000000000000 1 1 1 mag 1.000000000000 2.000000000000 3.000000000000" in stru_c)
        return [tdir]

    def test_prepare(self):
        op = PrepAbacus()
        out = op.execute(
            OPIO(
                {
                    "prep_image_config" : {},
                    "optional_artifact" : {"TEST": (self.source_path/'optional_test').absolute()},
                    "confs" : self.confs,
                    "inputs" : self.abacus_inputs,
                    "type_map" : self.type_map,
                }
            )
        )

        tdirs = self.checkfile()

        self.assertEqual(tdirs, out['task_names'])
        self.assertEqual(tdirs, [str(ii) for ii in out['task_paths']])

        for ii in out['task_names']:
            self.assertEqual(Path(Path(ii)/'TEST').read_text(), "here test")
