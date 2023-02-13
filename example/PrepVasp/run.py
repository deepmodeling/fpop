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
from PrepRunFp.PrepFp.VASP.PrepVasp import PrepVasp
from PrepRunFp.PrepFp.VASP.VaspInputs import VaspInputs
upload_packages.append("../../PrepRunFp")

wf = Workflow(name = "test")
vi = VaspInputs(0.3,'test_incar',{'Na':'test_potcar'},True)
confs = [Path('data.000'),Path('data.001')]
vasp = Step(
    name="PrepVasp",
    template=PythonOPTemplate(PrepVasp,image="registry.dp.tech/dptech/prod-11461/phonopy:v1.2",command=["python3"]),
    artifacts={
        "confs":upload_artifact(confs),
        "optional_artifact":upload_artifact({"TEST":Path("test"),"ICONST":Path("iconst")}),
        },
    parameters={
        "config" : {},
        "inputs" : vi ,
        "optional_input" : {},
        "type_map" : ["Na"],
    }
) 
wf.add(vasp)
wf.submit()

import time
while wf.query_status() in ["Pending","Running"]:
    time.sleep(4)
assert(wf.query_status() == 'Succeeded')
step = wf.query_step(name="PrepVasp")[0]
download_artifact(step.outputs.artifacts["task_paths"])
