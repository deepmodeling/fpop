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
    argo_range,
    argo_len,
    argo_sequence,
)
from dflow.python import(
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    Slices,
)

from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.plugins.lebesgue import LebesgueExecutor
import os,sys
from typing import Optional, Set, List
from pathlib import Path
from copy import deepcopy

class PrepRunFp(Steps):
    def __init__(
        self,
        name : str,
        prep_op : OP,
        run_op : OP,
        prep_image : str,
        run_image : str,
        prep_config : Optional[dict] = None,
        run_config : Optional[dict] = None,
        upload_python_packages : Optional[List[os.PathLike]] = None,
    ):
        self._input_parameters = {
            "inputs" : InputParameter(),
            "type_map" : InputParameter(),
            "backward_list" : InputParameter(),
            "config" : InputParameter(type=dict , value={}),
            "optional_input" : InputParameter(type=dict , value={}),
            "log_name" : InputParameter(type=str , value="log"),
            "backward_dir_name" : InputParameter(type=str , value="backward_dir"),
        }
        self._input_artifacts = {
            "confs" : InputArtifact(),
            "optional_artifact" : InputArtifact(optional=True), 
        }
        self._output_artifacts = {
            "backward_dirs" : OutputArtifact(),
        }

        super().__init__(
            name=name,
            inputs=Inputs(
                parameters=self._input_parameters,
                artifacts=self._input_artifacts,
            ),
            outputs=Outputs(
                artifacts=self._output_artifacts,
            )
        )

        self._keys = ['prep-fp','run-fp']
        self.step_keys = {'prep-fp':'prep-fp','run-fp':'run-fp'}

        self = _prep_run_fp(
            self,
            self.step_keys,
            prep_op,
            run_op,
            prep_image,
            run_image,
            prep_config,
            run_config,
            upload_python_packages = upload_python_packages,
        )

    @property
    def input_parameters(self):
        return self._input_parameters

    @property
    def input_artifacts(self):
        return self._input_artifacts

    @property
    def output_artifacts(self):
        return self._output_artifacts

    @property
    def keys(self):
        return self._keys

def _prep_run_fp(
        prep_run_steps,
        step_keys,
        prep_op : OP,
        run_op : OP,
        prep_image,
        run_image,
        prep_config : Optional[dict] = None,
        run_config : Optional[dict] = None,
        upload_python_packages : Optional[List[os.PathLike]] = None,
):
    prep_config = deepcopy(prep_config)
    run_config = deepcopy(run_config)
    if run_config:
        command = run_config.pop("command") if "command" in run_config.keys() else None
    prep_template_config = prep_config.pop('template_config') if 'template_config' in prep_config.keys() else {}
    run_template_config = run_config.pop('template_config') if 'template_config' in run_config.keys() else {}

    prep_fp = Step(
        'prep-fp' , 
        template=PythonOPTemplate(
            prep_op,
            output_artifact_archive={
                "task_paths": None
            },
            python_packages = upload_python_packages,
            image = prep_image
            **prep_template_config,
        ),
        parameters={
            "config" : prep_run_steps.inputs.parameters["config"],
            "inputs" : prep_run_steps.inputs.parameters["inputs"],
            "type_map" : prep_run_steps.inputs.parameters["type_map"],
            "optional_input" : prep_run_steps.inputs.parameters["optional_input"],
        },
        artifacts={
            "confs" : prep_run_steps.inputs.artifacts['confs'],
            "optional_artifact" : prep_run_steps.inputs.artifacts['optional_artifact'],
        },
        key = step_keys['prep-fp'],
        **prep_config,    
    )
    prep_run_steps.add(prep_fp)

    run_fp = Step(
        'run-fp',
        template=PythonOPTemplate(
            run_op,
            slices = Slices(
                "int('{{item}}')",
                input_parameter = ["task_name"],
                input_artifact = ["task_path"],
                output_artifact = ["backward_dir"],
            ),
            python_packages = upload_python_packages,
            image = run_image
            **run_template_config,
        ),
        parameters={
            "config" : prep_run_steps.inputs.parameters["config"],
            "task_name" : prep_fp.outputs.parameters["task_names"],
            "backward_list" : prep_run_steps.inputs.parameters["backward_list"],
            "log_name" : prep_run_steps.inputs.parameters["log_name"],
            "backward_dir_name" : prep_run_steps.inputs.parameters["backward_dir_name"],
            "optional_input" : prep_run_steps.inputs.parameters["optional_input"],
        },
        artifacts={
            "task_path" : prep_fp.outputs.artifacts['task_paths'],
            "optional_artifact" : prep_run_steps.inputs.artifacts["optional_artifact"],
        },
        with_sequence=argo_sequence(argo_len(prep_fp.outputs.parameters["task_names"]), format='%06d'),
        key = step_keys['run-fp'],
        **run_config,
    )
    prep_run_steps.add(run_fp)

    prep_run_steps.outputs.artifacts["backward_dirs"]._from = run_fp.outputs.artifacts["backward_dir"]
