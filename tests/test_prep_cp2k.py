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
from fpop.cp2k import PrepCp2k,Cp2kInputs
from typing import List
from constants import POSCAR_1_content,POSCAR_2_content,dump_conf_from_poscar
upload_packages.append("../fpop")
upload_packages.append("./context.py")

def check_cp2k_tasks(tcase, ntasks):
    cc = 0
    tdirs = []
    for ii in range (ntasks):
        tdir = "task.%06d" % cc
        tdirs.append(tdir)
        print(tdir)
        tcase.assertTrue(Path(tdir).is_dir())
        input = Path(tdir)/'input.inp'
        coord = Path(tdir)/'coord.xyz'
        cell = Path(tdir)/'CELL_PARAMETER'
        tcase.assertTrue(input.is_file())
        tcase.assertTrue(coord.is_file())
        tcase.assertTrue(cell.is_file())
        tcase.assertEqual(input.read_text(),'&CP2K_INPUT\n&END CP2K_INPUT')
        cc += 1
    return tdirs

class TestPrepCp2kDpConf(unittest.TestCase):
    '''
    deepmd/npy format named ["data.000","data.001"].
    no optional_input or optional_artifact.
    '''
    def setUp(self):
        self.ntasks = 2
        confs = dump_conf_from_poscar("deepmd/npy",[POSCAR_1_content, POSCAR_2_content])
        self.confs = [Path(ii) for ii in confs]
        self.inp_file = 'input.inp'
        Path(self.inp_file).write_text('&CP2K_INPUT\n&END CP2K_INPUT')
        self.type_map = ['Na']
    
    def tearDown(self):
        for ii in range(self.ntasks):
            work_path = Path("task.%06d"%ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)
        if Path(self.inp_file).is_file():
            os.remove(self.inp_file)
        for ii in self.confs:
            if ii.is_dir():
                shutil.rmtree(ii)

    def test(self):
        op = PrepCp2k()
        cp2k_inputs = Cp2kInputs(self.inp_file)
        out = op.execute(
            OPIO(
                {
                    "prep_image_config" : {},
                    "optional_input" : {},
                    "confs" : self.confs,
                    "inputs" : cp2k_inputs,
                    "type_map" : self.type_map,
                }
            )
        )
        tdirs = check_cp2k_tasks(self, self.ntasks)
        self.assertEqual(tdirs, out['task_names'])
        self.assertEqual(tdirs, [str(ii) for ii in out['task_paths']])
     
    def testWithoutOptionalParameter(self):
        op = PrepCp2k()
        cp2k_inputs = Cp2kInputs(self.inp_file)
        out = op.execute(
            OPIO(
                {
                    "confs" : self.confs,
                    "inputs" : cp2k_inputs,
                    "type_map" : self.type_map,
                }
            )
        )
        tdirs = check_cp2k_tasks(self, self.ntasks)
        self.assertEqual(tdirs, out['task_names'])
        self.assertEqual(tdirs, [str(ii) for ii in out['task_paths']])
    
@unittest.skipIf(skip_ut_with_dflow, skip_ut_with_dflow_reason)
class TestPrepRunCp2kPoscarConf(unittest.TestCase):
    '''
    vasp/poscar format named ["POSCAR_1","POSCAR_2","POSCAR_3"].
    Add optional_input["conf_format"] and optional_artifact. 
    '''
    def setUp(self):
        self.ntasks = 2
        confs = dump_conf_from_poscar("deepmd/npy",[POSCAR_1_content, POSCAR_2_content])
        self.confs = [Path(ii) for ii in confs]
        self.inp_file = 'input.inp'
        Path(self.inp_file).write_text('&CP2K_INPUT\n&END CP2K_INPUT')
        self.type_map = ['Na']
        self.optional_file = Path('optional_test')
        self.optional_file.write_text('here test')

    def tearDown(self):
        for ii in range(self.ntasks):
            work_path = Path("task.%06d"%ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)
        for ii in [self.inp_file,self.optional_file]:
            if Path(ii).is_file:
                os.remove(ii)
        if not self.confs:
            for ii in self.confs:
                if ii.is_file:
                    os.remove(ii)

    def test(self):
        wf = Workflow(name = "test")
        ci = Cp2kInputs(self.inp_file)
        cp2k = Step(
            name="PrepCp2k",
            template=PythonOPTemplate(PrepCp2k,image=default_image),
            artifacts={
                "confs":upload_artifact(self.confs),
                "optional_artifact":upload_artifact({"TEST":Path("optional_test")}),
            },
            parameters={
                "prep_image_config" : {},
                "inputs" : ci ,
                "optional_input" : {},
                "type_map" : self.type_map,
            }
        ) 
        wf.add(cp2k)
        wf.submit()

        while wf.query_status() in ["Pending","Running"]:
            time.sleep(4)
        assert(wf.query_status() == 'Succeeded')
        step = wf.query_step(name="PrepCp2k")[0]
        download_artifact(step.outputs.artifacts["task_paths"])

        tdirs = check_cp2k_tasks(self, self.ntasks)
        self.assertEqual(tdirs, step.outputs.parameters['task_names'].value)
        
        #check optional_artifact
        for ii in step.outputs.parameters['task_names'].value:
            self.assertEqual(Path(Path(ii)/'TEST').read_text(), "here test")

    def testWithoutOptionalParameter(self):
        wf = Workflow(name = "test")
        ci = Cp2kInputs(self.inp_file)
        cp2k = Step(
            name="PrepCp2k",
            template=PythonOPTemplate(PrepCp2k,image=default_image),
            artifacts={
                "confs":upload_artifact(self.confs),
                "optional_artifact":upload_artifact({"TEST":Path("optional_test")}),
            },
            parameters={
                "inputs" : ci ,
                "type_map" : self.type_map,
            }
        ) 
        wf.add(cp2k)
        wf.submit()

        while wf.query_status() in ["Pending","Running"]:
            time.sleep(4)
        assert(wf.query_status() == 'Succeeded')
        step = wf.query_step(name="PrepCp2k")[0]
        download_artifact(step.outputs.artifacts["task_paths"])

        tdirs = check_cp2k_tasks(self, self.ntasks)
        self.assertEqual(tdirs, step.outputs.parameters['task_names'].value)
        
        #check optional_artifact
        for ii in step.outputs.parameters['task_names'].value:
            self.assertEqual(Path(Path(ii)/'TEST').read_text(), "here test")
