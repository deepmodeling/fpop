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
from fpop.vasp import PrepVasp,VaspInputs
from typing import List
from constants import POSCAR_1_content,POSCAR_2_content,dump_conf_from_poscar
upload_packages.append("../fpop")
upload_packages.append("./context.py")

def check_vasp_tasks(tcase, ntasks):
    cc = 0
    tdirs = []
    for ii in range (ntasks):
        tdir = "task.%06d" % cc
        tdirs.append(tdir)
        print(tdir)
        tcase.assertTrue(Path(tdir).is_dir())
        incar = Path(tdir)/'INCAR'
        poscar = Path(tdir)/'POSCAR'
        potcar = Path(tdir)/'POTCAR'
        kpoints = Path(tdir)/'KPOINTS'
        tcase.assertTrue(incar.is_file())
        tcase.assertTrue(poscar.is_file())
        tcase.assertTrue(potcar.is_file())
        tcase.assertTrue(kpoints.is_file())
        tcase.assertEqual(incar.read_text(),'here incar')
        tcase.assertEqual(potcar.read_text(),'here potcar')
        cc += 1
    return tdirs

class TestPrepVaspDpConf(unittest.TestCase):
    '''
    deepmd/npy format named ["data.000","data.001"].
    no optional_input or optional_artifact.
    '''
    def setUp(self):
        self.ntasks = 2
        confs = dump_conf_from_poscar("deepmd/npy",[POSCAR_1_content, POSCAR_2_content])
        self.confs = [Path(ii) for ii in confs]
        self.incar = 'incar'
        Path(self.incar).write_text('here incar')
        self.potcar = 'potcar'
        Path(self.potcar).write_text('here potcar')
        self.type_map = ['Na']
    
    def tearDown(self):
        for ii in range(self.ntasks):
            work_path = Path("task.%06d"%ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)
        for ii in [self.incar,self.potcar]:
            if Path(ii).is_file:
                os.remove(ii)
        for ii in self.confs:
            if ii.is_dir():
                shutil.rmtree(ii)

    def test(self):
        op = PrepVasp()
        vasp_inputs = VaspInputs(
            0.3,
            self.incar,
            {'Na':self.potcar},
            True,
        )
        out = op.execute(
            OPIO(
                {
                    "prep_image_config" : {},
                    "optional_input" : {},
                    "confs" : self.confs,
                    "inputs" : vasp_inputs,
                    "type_map" : self.type_map,
                }
            )
        )
        tdirs = check_vasp_tasks(self, self.ntasks)
        self.assertEqual(tdirs, out['task_names'])
        self.assertEqual(tdirs, [str(ii) for ii in out['task_paths']])
     
    def testWithoutOptionalParameter(self):
        op = PrepVasp()
        vasp_inputs = VaspInputs(
            0.3,
            self.incar,
            {'Na':self.potcar},
            True,
        )
        out = op.execute(
            OPIO(
                {
                    "confs" : self.confs,
                    "inputs" : vasp_inputs,
                    "type_map" : self.type_map,
                }
            )
        )
        tdirs = check_vasp_tasks(self, self.ntasks)
        self.assertEqual(tdirs, out['task_names'])
        self.assertEqual(tdirs, [str(ii) for ii in out['task_paths']])
    
@unittest.skipIf(skip_ut_with_dflow, skip_ut_with_dflow_reason)
class TestPrepRunVaspPoscarConf(unittest.TestCase):
    '''
    vasp/poscar format named ["POSCAR_1","POSCAR_2","POSCAR_3"].
    Add optional_input["conf_format"] and optional_artifact. 
    '''
    def setUp(self):
        self.ntasks = 2
        confs = dump_conf_from_poscar("deepmd/npy",[POSCAR_1_content, POSCAR_2_content])
        self.confs = [Path(ii) for ii in confs]
        self.incar = 'incar'
        Path(self.incar).write_text('here incar')
        self.potcar = 'potcar'
        Path(self.potcar).write_text('here potcar')
        self.type_map = ['Na']
        self.optional_file = Path('optional_test')
        self.optional_file.write_text('here test')

    def tearDown(self):
        for ii in range(self.ntasks):
            work_path = Path("task.%06d"%ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)
        for ii in [self.incar,self.potcar,self.optional_file]:
            if Path(ii).is_file:
                os.remove(ii)
        if not self.confs:
            for ii in self.confs:
                if ii.is_file:
                    os.remove(ii)

    def test(self):
        wf = Workflow(name = "test")
        vi = VaspInputs(0.3,self.incar,{'Na':self.potcar},True)
        vasp = Step(
            name="PrepVasp",
            template=PythonOPTemplate(PrepVasp,image=default_image),
            artifacts={
                "confs":upload_artifact(self.confs),
                "optional_artifact":upload_artifact({"TEST":Path("optional_test")}),
            },
            parameters={
                "prep_image_config" : {},
                "inputs" : vi ,
                "optional_input" : {},
                "type_map" : self.type_map,
            }
        ) 
        wf.add(vasp)
        wf.submit()

        while wf.query_status() in ["Pending","Running"]:
            time.sleep(4)
        assert(wf.query_status() == 'Succeeded')
        step = wf.query_step(name="PrepVasp")[0]
        download_artifact(step.outputs.artifacts["task_paths"])

        tdirs = check_vasp_tasks(self, self.ntasks)
        self.assertEqual(tdirs, step.outputs.parameters['task_names'].value)
        
        #check optional_artifact
        for ii in step.outputs.parameters['task_names'].value:
            self.assertEqual(Path(Path(ii)/'TEST').read_text(), "here test")

    def testWithoutOptionalParameter(self):
        wf = Workflow(name = "test")
        vi = VaspInputs(0.3,self.incar,{'Na':self.potcar},True)
        vasp = Step(
            name="PrepVasp",
            template=PythonOPTemplate(PrepVasp,image=default_image),
            artifacts={
                "confs":upload_artifact(self.confs),
                "optional_artifact":upload_artifact({"TEST":Path("optional_test")}),
            },
            parameters={
                "inputs" : vi ,
                "type_map" : self.type_map,
            }
        ) 
        wf.add(vasp)
        wf.submit()

        while wf.query_status() in ["Pending","Running"]:
            time.sleep(4)
        assert(wf.query_status() == 'Succeeded')
        step = wf.query_step(name="PrepVasp")[0]
        download_artifact(step.outputs.artifacts["task_paths"])

        tdirs = check_vasp_tasks(self, self.ntasks)
        self.assertEqual(tdirs, step.outputs.parameters['task_names'].value)
        
        #check optional_artifact
        for ii in step.outputs.parameters['task_names'].value:
            self.assertEqual(Path(Path(ii)/'TEST').read_text(), "here test")

