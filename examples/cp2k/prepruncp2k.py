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
import fpop
from fpop.cp2k import PrepCp2k,Cp2kInputs,RunCp2k
from fpop.preprun_fp import PrepRunFp
from dflow.plugins.dispatcher import DispatcherExecutor

prep_template_config={}
prep_step_config={}
run_template_config={}
run_slice_config={}
run_step_config={
        "executor":{
            "type": "dispatcher",
            "machine_dict":{
                "batch_type": "Bohrium",
                "context_type": "Bohrium",
                "remote_profile": {
                    "input_data": {
                        "job_type": "container",
                        "platform": "ali",
                        "scass_type": "c16_m32_cpu",
                        }
                    }
                }
            }
    }
prep_image_config={}
run_image_config={
    "command": "ulimit -s unlimited && ulimit -m unlimited && mpirun -n 16 --allow-run-as-root --oversubscribe cp2k.popt -i input.inp",
    }
upload_python_packages=list(fpop.__path__)

steps = PrepRunFp(
    "prep-run-cp2k",
    PrepCp2k,
    RunCp2k,
    "registry.dp.tech/dptech/prod-15842/ubuntu:22.04-py3.10-ase",
    "registry.dp.tech/dptech/prod-25571/initreaction:cp2k0410",
    prep_template_config,
    prep_step_config,
    run_template_config,
    run_slice_config,
    run_step_config,
    upload_python_packages, 
)

confs = ["POSCAR-1","POSCAR-2","POSCAR-3"]


cp2k_inputs = Cp2kInputs('input.inp') # input files
prep_run_step = Step(
    'prep-run-step', 
    template = steps,
    parameters = {
        'type_map' : ["Na"],
        'prep_image_config' : prep_image_config,
        'run_image_config' : run_image_config,
        "log_name" : "output.log",
        'inputs' : cp2k_inputs,
        'optional_input' : {"conf_format":"vasp/poscar"},
        'backward_list' : ["output.log"],
    },
    artifacts = {
        "confs" : upload_artifact(confs),
    },
)

wf = Workflow(name="prepruncp2k")
wf.add(prep_run_step)
wf.submit()

import time
while wf.query_status() in ["Pending", "Running"]:
    time.sleep(4)
assert(wf.query_status() == 'Succeeded')
step = wf.query_step(name="prep-run-step")[0]
download_artifact(step.outputs.artifacts["backward_dirs"])
