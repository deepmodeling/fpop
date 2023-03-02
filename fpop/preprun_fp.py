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
import os,sys
from typing import Optional, Set, List
from pathlib import Path
from fpop.utils.step_config import (
    init_executor,
)

class PrepRunFp(Steps):
    def __init__(
        self,
        name : str,
        prep_op : OP,
        run_op : OP,
        prep_image : str,
        run_image : str,
        prep_template_config : Optional[dict] = None,
        prep_step_config : Optional[dict] = None,
        run_template_config : Optional[dict] = None,
        run_slice_config : Optional[dict] = None,
        run_step_config : Optional[dict] = None,
        upload_python_packages : Optional[List[str]] = None,
    ):
        self._input_parameters = {
            "inputs" : InputParameter(),
            "type_map" : InputParameter(),
            "backward_list" : InputParameter(),
            "prep_image_config" : InputParameter(type=dict , value={}),
            "run_image_config" : InputParameter(type=dict , value={}),
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
        self.step_keys = {'prep-fp':'prep-fp','run-fp':'run-fp-{{item}}'}

        self = _prep_run_fp(
            self,
            self.step_keys,
            prep_op,
            run_op,
            prep_image,
            run_image,
            prep_template_config,
            prep_step_config,
            run_template_config,
            run_slice_config,
            run_step_config,
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
        prep_template_config : Optional[dict] = None,
        prep_step_config : Optional[dict] = None,
        run_template_config : Optional[dict] = None,
        run_slice_config : Optional[dict] = None,
        run_step_config : Optional[dict] = None,
        upload_python_packages : Optional[List[str]] = None,
):
    if not prep_template_config: prep_template_config = {}
    if not prep_step_config: prep_step_config = {}
    if not run_template_config: run_template_config = {}
    if not run_slice_config: run_slice_config = {}
    if not run_step_config: run_step_config = {}
    if "executor" in prep_step_config.keys():
        prep_executor = init_executor(prep_step_config.pop("executor"))
    else:
        prep_executor = None
    if "executor" in run_step_config.keys():
        run_executor = init_executor(run_step_config.pop("executor"))
    else:
        run_executor = None

    prep_fp = Step(
        'prep-fp' , 
        template=PythonOPTemplate(
            prep_op,
            output_artifact_archive={
                "task_paths": None
            },
            python_packages = upload_python_packages,
            image = prep_image,
            **prep_template_config,
        ),
        parameters={
            "prep_image_config" : prep_run_steps.inputs.parameters["prep_image_config"],
            "inputs" : prep_run_steps.inputs.parameters["inputs"],
            "type_map" : prep_run_steps.inputs.parameters["type_map"],
            "optional_input" : prep_run_steps.inputs.parameters["optional_input"],
        },
        artifacts={
            "confs" : prep_run_steps.inputs.artifacts['confs'],
            "optional_artifact" : prep_run_steps.inputs.artifacts['optional_artifact'],
        },
        key = step_keys['prep-fp'],
        executor = prep_executor,
        **prep_step_config,    
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
                **run_slice_config,
            ),
            python_packages = upload_python_packages,
            image = run_image,
            **run_template_config,
        ),
        parameters={
            "run_image_config" : prep_run_steps.inputs.parameters["run_image_config"],
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
        executor = run_executor,
        **run_step_config,
    )
    prep_run_steps.add(run_fp)

    prep_run_steps.outputs.artifacts["backward_dirs"]._from = run_fp.outputs.artifacts["backward_dir"]
