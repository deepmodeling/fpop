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
    ShellOPTemplate
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
from PrepRunFp.RunFp.VASP.RunVasp import RunVasp
from dflow.plugins.dispatcher import DispatcherExecutor
upload_packages.append("../../PrepRunFp")

dispatcher_executor_cpu = DispatcherExecutor(
        machine_dict={
            "batch_type": "Bohrium",
            "context_type": "Bohrium",
            "remote_profile": {
                "email": "xxx",
                "password": "xxx",
                "program_id": 123,
                "input_data": {
                    "job_type": "container",
                    "platform": "ali",
                    "scass_type": "c16_m32_cpu",
                },
            },
        },
    )

wf = Workflow(name = "test")
vasp = Step(
    name="VASP",
    template=PythonOPTemplate(RunVasp,image="registry.dp.tech/dptech/vasp:5.4.4-dflow",command=["python3"]),
    artifacts={
        "task_path":upload_artifact(Path("vasp_test")),
        "optional_artifact": upload_artifact({'abc':Path('efg')}),
        },
    parameters={
        "config": {
            "run": {
                "command": "source /opt/intel/oneapi/setvars.sh && ulimit -s unlimited && mpirun -n 64 /opt/vasp.5.4.4/bin/vasp_std ",
                }
            },
        "task_name": "vasp_test",
        "backward_list": ["OUTCAR"],
    },
    executor=dispatcher_executor_cpu,
    )
wf.add(vasp)
wf.submit()

import time
while wf.query_status() in ["Pending","Running"]:
    time.sleep(4)
assert(wf.query_status() == 'Succeeded')
step = wf.query_step(name="VASP")[0]
download_artifact(step.outputs.artifacts["backward_dir"])
