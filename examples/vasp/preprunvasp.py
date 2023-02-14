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
from fpop.vasp import PrepVasp,VaspInputs,RunVasp
from fpop.preprun_fp import PrepRunFp
from dflow.plugins.dispatcher import DispatcherExecutor
upload_packages.append("../../fpop")
from copy import deepcopy

fp_config={
    "prep":{
        "template_config": {
            "image": "dptechnology/dpgen2:latest",
        },
    },
    "run":{
        "command": "source /opt/intel/oneapi/setvars.sh && ulimit -s unlimited && mpirun -n 64 /opt/vasp.5.4.4/bin/vasp_std",
        "template_config": {
            "image": "",
        },
        "executor":{
            "type":"dispatcher",
            "machine_dict":{
                "batch_type": "",
                "context_type": "",
                "remote_profile": {
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

confs = ["POSCAR"]

vasp_inputs = VaspInputs(0.2,'test_incar',{'Na':'test_potcar'},True) # kspacing  incar  potcar kgamma
prep_run_step = Step(
    'prep-run-step', 
    template = steps,
    parameters = {
        'type_map' : ["Na"],
        'config' : fp_config,
        'inputs' : vasp_inputs,
        'optional_input' : {"conf_format":"vasp/poscar"},
        'backward_list' : ["OUTCAR"],
    },
    artifacts = {
        "confs" : upload_artifact(confs),
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
