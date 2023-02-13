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

import time, shutil, json, jsonpickle
from pathlib import Path
    
from context import (
        PrepRunFp,
        default_image,
        upload_python_packages,
        skip_ut_with_dflow,
        skip_ut_with_dflow_reason,
        )
from PrepRunFp.PrepFp.VASP.PrepVasp import PrepVasp
from PrepRunFp.PrepFp.VASP.VaspInputs import VaspInputs

from mocked_ops import MockedRunVasp
from PrepRunFp.PrepRunFp import PrepRunFp
upload_packages.append("../PrepRunFp")
upload_packages.append("./context.py")

default_config = {
        "prep":{
            "template_config" : {
                "image" : default_image,
            },  
        },
        "run":{
            "template_config" : {
                "image" : default_image,
            },  
        },
    }

class TestMockedRunVasp(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter('ignore', ResourceWarning)
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

@unittest.skipIf(skip_ut_with_dflow, skip_ut_with_dflow_reason)
class TestPrepRunVasp(unittest.TestCase):
    def setUp(self):
        warnings.simplefilter('ignore', ResourceWarning)
        self.ntasks = 3
        self.confs = [Path(Path('confs')/'data.000'),Path(Path('confs')/'data.001')]
        self.incar = 'incar'
        self.incar.write_text("This is INCAR")
        self.potcar = 'potcar'
        self.potcar.write_text('This is POTCAR')
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
            if ii.is_file():
                os.remove(ii)

    def test(self):
        steps = PrepRunFp(
            "prep-run-vasp",
            PrepVasp,
            MockedRunVasp,
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
