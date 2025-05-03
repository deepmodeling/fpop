import os
import numpy as np
import unittest

from dflow import (
    InputParameter,
    OutputParameter,
    Inputs,
    InputArtifact,
    Outputs,
    OutputArtifact,
    Workflow,
    Step,
    Steps,
    upload_artifact,
    download_artifact,
    S3Artifact,
    argo_range
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    upload_packages,
)

import time, shutil, dpdata
from pathlib import Path

from context import (
        fpop,
        default_image,
        upload_python_packages,
        skip_ut_with_dflow,
        skip_ut_with_dflow_reason,
        )
from fpop.abacus import PrepAbacus,AbacusInputs
from typing import List
from constants import POSCAR_1_content,POSCAR_2_content,STRU1_content,dump_conf_from_poscar
upload_packages.append("../fpop")
upload_packages.append("./context.py")


class TestPrepAbacus(unittest.TestCase):
    '''
    deepmd/npy format named ["data.000","data.001"].
    no optional_input or optional_artifact.
    '''
    def setUp(self):
        self.ntasks = 2
        confs = dump_conf_from_poscar("deepmd/npy",[POSCAR_1_content, POSCAR_2_content])
        self.confs = [Path(ii) for ii in confs]
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
        for ii in self.confs:
            if ii.is_dir():
                shutil.rmtree(ii)

    def checkfile(self):
        tdirs = []
        for ii in range (self.ntasks):
            tdir = "task.%06d" % ii
            tdirs.append(tdir)
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
        return tdirs

class TestPrepAbacusDpConf(TestPrepAbacus,unittest.TestCase):
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
     
    def testWithoutOptionalArtifact(self):
        op = PrepAbacus()
        out = op.execute(
            OPIO(
                {
                    "confs" : self.confs,
                    "inputs" : self.abacus_inputs,
                    "type_map" : self.type_map,
                }
            )
        )

        tdirs = self.checkfile()

        self.assertEqual(tdirs, out['task_names'])
        self.assertEqual(tdirs, [str(ii) for ii in out['task_paths']])

   
@unittest.skipIf(skip_ut_with_dflow, skip_ut_with_dflow_reason)
class TestPrepAbacusConf(TestPrepAbacus,unittest.TestCase):
    def test_prepare(self):
        wf = Workflow(name = "test")
        abacus = Step(
            name="PrepAbacus",
            template=PythonOPTemplate(PrepAbacus,image=default_image),
            artifacts={
                "confs":upload_artifact(self.confs),
                "optional_artifact":upload_artifact({"TEST":(self.source_path/'optional_test')}),
            },
            parameters={
                "prep_image_config" : {},
                "inputs" : self.abacus_inputs ,
                "optional_input" : {},
                "type_map" : self.type_map,
            }
        ) 
        wf.add(abacus)
        wf.submit()

        while wf.query_status() in ["Pending","Running"]:
            time.sleep(4)
        assert(wf.query_status() == 'Succeeded')
        step = wf.query_step(name="PrepAbacus")[0]
        download_artifact(step.outputs.artifacts["task_paths"])

        tdirs = self.checkfile()

        self.assertEqual(tdirs, step.outputs.parameters['task_names'].value)
        
        #check optional_artifact
        for ii in step.outputs.parameters['task_names'].value:
            self.assertEqual(Path(Path(ii)/'TEST').read_text(), "here test")

    def testWithoutOptionalParam(self):
        wf = Workflow(name = "test")
        abacus = Step(
            name="PrepAbacus",
            template=PythonOPTemplate(PrepAbacus,image=default_image),
            artifacts={
                "confs":upload_artifact(self.confs),
                "optional_artifact":upload_artifact({"TEST":(self.source_path/'optional_test')})
            },
            parameters={
                "inputs" : self.abacus_inputs ,
                "type_map" : self.type_map,
            }
        ) 
        wf.add(abacus)
        wf.submit()

        while wf.query_status() in ["Pending","Running"]:
            time.sleep(4)
        assert(wf.query_status() == 'Succeeded')
        step = wf.query_step(name="PrepAbacus")[0]
        download_artifact(step.outputs.artifacts["task_paths"])

        tdirs = self.checkfile()

        self.assertEqual(tdirs, step.outputs.parameters['task_names'].value)

class TestPrepAbacusSpin(unittest.TestCase):
    '''
    deepmd/npy format named ["data.000","data.001"].
    no optional_input or optional_artifact.
    '''
    def setUp(self):
        self.source_path = Path('abacustest')
        self.word_path = Path('task.000')
        self.source_path.mkdir(parents=True, exist_ok=True)
        self.word_path.mkdir(parents=True, exist_ok=True)
        
        stru_path = self.source_path / "STRU_tmp"
        stru_path.write_text(STRU1_content)
        self.confs = dpdata.System(str(stru_path), fmt="abacus/stru")
        self.confs.data["spins"] = [[[1,2,3],[4,5,6],[7,8,9],[1,1,1],
                                     [2,2,2],[3,3,3],[4,4,4],[5,5,5]]]
        
        (self.source_path/"INPUT").write_text('INPUT_PARAMETERS\ncalculation scf\nbasis_type lcao\n')
        (self.source_path/"KPT").write_text('here kpt')
        (self.source_path/"As.upf").write_text('here As upf')
        (self.source_path/"Ga.upf").write_text('here Ga upf')
        (self.source_path/"As.orb").write_text('here As orb')
        (self.source_path/"Ga.orb").write_text('here Ga orb')
        (self.source_path/'optional_test').write_text('here test')

        self.abacus_inputs = AbacusInputs(
            input_file=self.source_path/"INPUT",
            kpt_file=self.source_path/"KPT",
            pp_files={"As":self.source_path/"As.upf","Ga":self.source_path/"Ga.upf"},
            orb_files={"As":self.source_path/"As.orb","Ga":self.source_path/"Ga.orb"},
            constrain_elements=["As"] # only constrain As
        )
    
    def tearDown(self):
        if os.path.isdir(self.source_path):
            shutil.rmtree(self.source_path)
        if os.path.isdir(self.word_path):
            shutil.rmtree(self.word_path)

    def test_prepare_abacus_spin(self):
        pabacus = PrepAbacus()
        os.chdir(self.word_path)
        
        pabacus.prep_task(self.confs, self.abacus_inputs)

        self.assertTrue(os.path.isfile('INPUT'))
        self.assertTrue(os.path.isfile('KPT'))
        self.assertTrue(os.path.isfile('STRU'))
        self.assertTrue(os.path.isfile('As.upf'))
        self.assertTrue(os.path.isfile('Ga.upf'))
        self.assertTrue(os.path.isfile('As.orb'))
        self.assertTrue(os.path.isfile('Ga.orb'))
        self.assertEqual(Path('INPUT').read_text().split()[0],"INPUT_PARAMETERS")      
        self.assertEqual(Path('KPT').read_text(),'here kpt')
        self.assertEqual(Path('As.upf').read_text(),'here As upf')
        self.assertEqual(Path('Ga.upf').read_text(),'here Ga upf')
        self.assertEqual(Path('As.orb').read_text(),'here As orb')
        self.assertEqual(Path('Ga.orb').read_text(),'here Ga orb')
            
        with open("STRU") as f : c = f.read()
        #print(c)
        ref_c = '''Ga
0.0
4
0.000000000000 0.000000000000 0.000000000000 1 1 1 mag 1.000000000000 2.000000000000 3.000000000000
0.000000000000 2.875074596069 2.875074596069 1 1 1 mag 4.000000000000 5.000000000000 6.000000000000
2.875074596069 0.000000000000 2.875074596069 1 1 1 mag 7.000000000000 8.000000000000 9.000000000000
2.875074596069 2.875074596069 0.000000000000 1 1 1 mag 1.000000000000 1.000000000000 1.000000000000
As
0.0
4
1.437537298035 1.437537298035 1.437537298035 1 1 1 mag 2.000000000000 2.000000000000 2.000000000000 sc 1 1 1
1.437537298035 4.312611894104 4.312611894104 1 1 1 mag 3.000000000000 3.000000000000 3.000000000000 sc 1 1 1
4.312611894104 1.437537298035 4.312611894104 1 1 1 mag 4.000000000000 4.000000000000 4.000000000000 sc 1 1 1
4.312611894104 4.312611894104 1.437537298035 1 1 1 mag 5.000000000000 5.000000000000 5.000000000000 sc 1 1 1'''
        self.assertTrue(ref_c in c)
        os.chdir("../")

