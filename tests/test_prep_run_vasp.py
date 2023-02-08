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
)

import time, shutil, json, jsonpickle
from typing import Set, List
from pathlib import Path

from context import PrepRunFp
from context import upload_python_packages
from PrepRunFp.PrepFp.VASP.PrepVasp import PrepVasp
from PrepRunFp.PrepFp.VASP.VaspInputs import VaspInputs
from mocked_ops import MockedRunVasp
from PrepRunFp.PrepRunFp import PrepRunFp

default_config = {
        "prep":{
            "template_config" : {
                "image" : 'registry.dp.tech/dptech/prod-11461/phonopy:v1.2',
            },  
        },
        "run":{
            "template_config" : {
                "image" : 'registry.dp.tech/dptech/prod-11461/phonopy:v1.2',
            },  
        },
    }

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
        tcase.assertEqual(incar.read_text(),'This is INCAR')
        tcase.assertEqual(potcar.read_text(),'This is POTCAR')
        cc += 1
    return tdirs

class TestPrepVaspDpConf(unittest.TestCase):
    '''
    Deepmd/npy format named ["data.000","data.001"].
    No optional_input or optional_artifact.
    '''
    def setUp(self):
        self.ntasks = 3
        self.confs = [Path('data.000'),Path('data.001')]
        self.incar = Path('incar')
        self.incar.write_text('This is INCAR')
        self.potcar = Path('potcar')
        self.potcar.write_text('This is POTCAR')
        self.type_map = ['Na']

    def tearDown(self):
        for ii in range(self.ntasks):
            work_path = Path("task.%06d"%ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)
        for ii in [self.incar,self.potcar]:
            if ii.is_file:
                os.remove(ii)

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
                    "config" : {},
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

class TestMockedRunVasp(unittest.TestCase):
    def setUp(self):
        self.ntask = 6
        self.task_list = []
        for ii in range(self.ntask):
            work_path = Path("task.%06d"%ii)
            work_path.mkdir(exist_ok=True,parents=True),
            for jj in ['INCAR','POTCAR','POSCAR','KPOINTS']:
                (work_path/jj).write_text(f'{jj} in task.%06d'%ii)
            self.task_list.append(work_path)

    def check_run_vasp_output(
        self,
        task_name : str,
    ):
        cwd = os.getcwd()
        os.chdir(task_name)
        fc = []
        for ii in ['POSCAR','INCAR']:
            fc.append(Path(ii).read_text())
        self.assertEqual(fc,Path(Path('my_backward')/'my_log').read_text().strip().split('\n'))
        self.assertTrue(Path(Path('my_backward')/'POSCAR').is_file())
        self.assertTrue(Path(Path('my_backward')/'POTCAR').is_file())
        fc = []
        fc.append(f"This is POSCAR which users need in {task_name}")
        fc.append(f"KPOINTS in {task_name}")
        self.assertEqual(Path(Path('my_backward')/'POSCAR').read_text().strip().split('\n'),fc)       
        os.chdir(cwd) 

    def tearDown(self):
        for ii in range(self.ntask):
            work_path = Path("task.%06d" % ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)

    def test(self):
        self.task_list_str = [str(ii) for ii in self.task_list]
        for ii in range(self.ntask):
            ip = OPIO({
                'task_name' : self.task_list_str[ii],
                'task_path' : self.task_list[ii],
                'log_name' : "my_log",
                'backward_dir_name' : "my_backward",
                "config" : {},
                "optional_artifact" : {},
                "backward_list" : ['POSCAR','POTCAR'],
                "optional_input" : {},
            })
            op = MockedRunVasp()
            out = op.execute(ip)
            self.assertEqual(out['backward_dir'] , Path(Path('task.%06d'%ii)/'my_backward'))
            self.assertTrue(Path(out['backward_dir'] / 'my_log').is_file())
            self.check_run_vasp_output(self.task_list_str[ii])

class TestPrepRunVasp(unittest.TestCase):
    def setUp(self):
        self.ntasks = 3
        self.confs = ["data.000","data.001"]
        self.incar = Path('incar')
        self.incar.write_text("This is INCAR")
        self.potcar = Path('potcar')
        self.potcar.write_text('This is POTCAR')
        self.type_map = ['Na']
    '''
    def tearDown(self):
        for ii in range(self.ntasks):
            work_path = Path("task.%06d" % ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)
        for ii in [self.incar, self.potcar]:
            if ii.is_file():
                os.remove(ii)
    '''
    def test(self):
        steps = PrepRunFp(
            "prep-run-vasp",
            PrepVasp,
            MockedRunVasp,
            upload_python_packages = upload_python_packages,
            prep_config = default_config["prep"],
            run_config = default_config["run"],
        )
        vasp_inputs = VaspInputs(
            0.3,
            self.incar,
            {'Na' : self.potcar},
            True,
        )
        prep_run_step = Step(
            'prep-run-step' , 
            template = steps,
            parameters = {
                'type_map' : self.type_map,
                'config' : default_config,
                'inputs' : vasp_inputs,
                'optional_input' : {},
                'backward_list' : ['POSCAR','POTCAR'],
            },
            artifacts = {
                "confs" : self.confs,
                "optional_artifact" : {},
            },
        )
        
        wf = Workflow(name="prerunvasp")
        wf.add(prep_run_step)
        wf.submit()

        while wf.query_status() in ["Pending", "Running"]:
            time.sleep(4)

        self.assertEqual(wf.query_status(), "Succeeded")
        step = wf.query_step(name="prep-run-step")[0]
        self.assertEqual(step.phase, "Succeeded")

        download_artifact(step.outputs.artifacts["backward_dirs"])
