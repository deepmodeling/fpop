from dflow import config, s3_config
from dflow.plugins import bohrium
from dflow.plugins.bohrium import TiefblueClient
config["host"] = "https://workflows.deepmodeling.com"
config["k8s_api_server"] = "https://workflows.deepmodeling.com"
bohrium.config["username"] = ""
bohrium.config["password"] = ""
bohrium.config["project_id"]
s3_config["repo_key"] = "oss-bohrium"
s3_config["storage_client"] = TiefblueClient()

import sys,os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),"../../")))
from dflow import (
    Workflow,
    Step,
    upload_artifact,
    download_artifact,
    InputArtifact,
    OutputArtifact,
    ShellOPTemplate,
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    TransientError,
    FatalError,
    BigParameter,
    upload_packages,
)
from pathlib import Path
from PrepRunFp.PrepFp.VASP.PrepVasp import PrepVasp
from PrepRunFp.PrepFp.VASP.VaspInputs import VaspInputs
from PrepRunFp.PrepRunFp import PrepRunFp
from PrepRunFp.RunFp.VASP.RunVasp import RunVasp
from dflow.plugins.dispatcher import DispatcherExecutor
upload_packages.append("../../PrepRunFp")
from copy import deepcopy

fp_config={
    "prep":{
        "template_config": {
            "image": "registry.dp.tech/dptech/prod-11461/phonopy:v1.2",
        },
    },
    "run":{
        "command": "source /opt/intel/oneapi/setvars.sh && ulimit -s unlimited && mpirun -n 64 /opt/vasp.5.4.4/bin/vasp_std",
        "template_config": {
            "image": "registry.dp.tech/dptech/vasp:5.4.4-dflow",
        },
        "executor":{
            "type":"dispatcher",
            "machine_dict":{
                "batch_type": "Bohrium",
                "context_type": "Bohrium",
                "remote_profile": {
                    "email":"123",
                    "password":"123", 
                    "program_id":123,
                    "input_data": {
                        "job_type": "container",
                        "platform": "ali",
                        "scass_type": "c16_m32_cpu",
                    },
                },
            },
        },
    },
}

prep_config = fp_config["prep"]
run_config = fp_config["run"]

steps = PrepRunFp(
    "prep-run-vasp",
    PrepVasp,
    RunVasp,
    prep_config,
    run_config,
)

confs = ["data.000","data.001"]

vasp_inputs = VaspInputs(5.0,'test_incar',{'Na':'test_potcar'},True) # kspacing  incar  potcar kgamma
prep_run_step = Step(
    'prep-run-step', 
    template = steps,
    parameters = {
        'type_map' : ["Na"],
        'config' : fp_config,
        'inputs' : vasp_inputs,
        'optional_input' : {},
        'backward_list' : ["OUTCAR"],
    },
    artifacts = {
        "confs" : upload_artifact(confs),
        "optional_artifact" : upload_artifact({"TEST":Path("test"),"ICONST":Path("iconst")}),
    },
)

wf = Workflow(name="preprunvasp")
wf.add(prep_run_step)
wf.submit()

import time
while wf.query_status() in ["Pending", "Running"]:
    time.sleep(4)
assert(wf.query_status() == 'Succeeded')
step = wf.query_step(name="prep-run-step")[0]
download_artifact(step.outputs.artifacts["backward_dirs"])
