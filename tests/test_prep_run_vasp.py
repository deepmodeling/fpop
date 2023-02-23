'''
If you want to do the UTs with dflow.You need export some environment variables on your machine.
export DFLOW_HOST=https://workflows.deepmodeling.com
export DFLOW_K8S_API_SERVER=https://workflows.deepmodeling.com
export DFLOW_S3_REPO_KEY=oss-bohrium
export DFLOW_S3_STORAGE_CLIENT=dflow.plugins.bohrium.TiefblueClient
export BOHRIUM_USERNAME=<bohrium-email>
export BOHRIUM_PASSWORD=<bohrium-password>
export BOHRIUM_PROJECT_ID=<bohrium-project-id>

If you don't want to do the UTs with dflow:
export SKIP_UT_WITH_DFLOW=1
'''

from dflow import config, s3_config
from dflow.plugins import bohrium
from dflow.plugins.bohrium import TiefblueClient
import os
import numpy as np
import unittest
import warnings

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

import time, shutil, json, jsonpickle, dpdata
from pathlib import Path
from typing import List
    
from context import (
        fpop,
        default_image,
        upload_python_packages,
        skip_ut_with_dflow,
        skip_ut_with_dflow_reason,
        )
from fpop.vasp import PrepVasp, VaspInputs

from mocked_ops import MockedRunVasp
from fpop.preprun_fp import PrepRunFp
from constants import POSCAR_1_content,POSCAR_2_content
upload_packages.append("../fpop")
upload_packages.append("./context.py")

def dump_conf_from_poscar(
        type, 
        conf_list
        ) -> List[str] :
    for ii in range(len(conf_list)):
        Path("POSCAR_%d"%ii).write_text(conf_list[ii])
    if type == "deepmd/npy":
        confs = []
        for ii in range(len(conf_list)):
            ls = dpdata.System("POSCAR_%d"%ii, fmt="vasp/poscar")
            ls.to_deepmd_npy("data.%03d"%ii)
            confs.append("data.%03d")
            os.remove("POSCAR_%d"%ii)
        return confs
    elif type == "vasp/poscar":
        confs = []
        for ii in range(len(conf_list)):
            confs.append("POSCAR_%d"%ii)
        return confs
    else:
        return []

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
                "run_image_config" : {},
                "optional_artifact" : {},
                "backward_list" : ['POSCAR','POTCAR'],
                "optional_input" : {},
            })
            op = MockedRunVasp()
            out = op.execute(ip)
            self.assertEqual(out['backward_dir'] , Path(Path('task.%06d'%ii)/'my_backward'))
            self.assertTrue(Path(out['backward_dir'] / 'my_log').is_file())
            self.check_run_vasp_output(self.task_list_str[ii])

    def testWithoutOptionalParameter(self):
        self.task_list_str = [str(ii) for ii in self.task_list]
        for ii in range(self.ntask):
            ip = OPIO({
                'task_name' : self.task_list_str[ii],
                'task_path' : self.task_list[ii],
                'log_name' : "my_log",
                'backward_dir_name' : "my_backward",
                "backward_list" : ['POSCAR','POTCAR'],
            })
            op = MockedRunVasp()
            out = op.execute(ip)
            self.assertEqual(out['backward_dir'] , Path(Path('task.%06d'%ii)/'my_backward'))
            self.assertTrue(Path(out['backward_dir'] / 'my_log').is_file())
            self.check_run_vasp_output(self.task_list_str[ii])

@unittest.skipIf(skip_ut_with_dflow, skip_ut_with_dflow_reason)
class TestPrepRunVasp(unittest.TestCase):
    def setUp(self):
        self.ntasks = 2
        confs = dump_conf_from_poscar("vasp/poscar",[POSCAR_1_content, POSCAR_2_content])
        self.confs = [Path(ii) for ii in confs]
        self.incar = 'incar'
        Path(self.incar).write_text("This is INCAR")
        self.potcar = 'potcar'
        Path(self.potcar).write_text('This is POTCAR')
        self.type_map = ['Na']
        self.optional_testfile = Path('test_file')
        self.optional_testfile.write_text('This is an optional artifact')

    def check_prep_run_vasp_output(
        self,
        task_name : str,
    ):
        cwd = os.getcwd()
        os.chdir(task_name)
        self.assertTrue(Path("my_backward").is_dir())
        for ii in ['POSCAR','POTCAR','TEST','my_log']:
            self.assertTrue(Path(Path("my_backward")/ii).is_file())
        os.chdir(cwd)
    
    def tearDown(self):
        for ii in range(self.ntasks):
            work_path = Path("task.%06d" % ii)
            if work_path.is_dir():
                shutil.rmtree(work_path)
        for ii in [self.incar, self.potcar, self.optional_testfile]:
            if Path(ii).is_file():
                os.remove(ii)
        for ii in self.confs:
            if ii.is_file():
                os.remove(ii)

    def test(self):
        steps = PrepRunFp(
            "prep-run-vasp",
            PrepVasp,
            MockedRunVasp,
            prep_image = default_image,
            run_image = default_image,
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
                'inputs' : vasp_inputs,
                'optional_input' : {"conf_format" : "vasp/poscar"},
                'backward_list' : ['POSCAR','POTCAR','TEST'],
                'backward_dir_name' : 'my_backward',
                'log_name' : 'my_log',
            },
            artifacts = {
                "confs" : upload_artifact(self.confs),
                "optional_artifact" : upload_artifact({'TEST':Path("test_file")}),
            },
        )
        
        wf = Workflow(name="prerunvasp")
        wf.add(prep_run_step)
        wf.submit()

        while wf.query_status() in ["Pending", "Running"]:
            time.sleep(4)

        assert(wf.query_status() == 'Succeeded')
        step = wf.query_step(name="prep-run-step")[0]
        download_artifact(step.outputs.artifacts["backward_dirs"])
        
        task_names = ["task.%06d" % ii for ii in range(self.ntasks)]
        for ii in task_names:
            self.check_prep_run_vasp_output(ii)
