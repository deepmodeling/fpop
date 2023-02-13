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
from dargs import (
    Argument,
    Variant,
)

from dflow.plugins.dispatcher import DispatcherExecutor
from dflow.plugins.lebesgue import LebesgueExecutor
import os,sys
from typing import Optional, Set, List
from pathlib import Path
from copy import deepcopy

def lebesgue_extra_args():
    # It is not possible to strictly check the keys in this section....
    doc_scass_type = "The machine configuraiton."
    doc_program_id = "The ID of the program."
    doc_job_type = "The type of job."
    doc_template_cover = "The key for hacking around a bug in Lebesgue."

    return [
        Argument("scass_type", str, optional=True, doc=doc_scass_type),
        Argument("program_id", str, optional=True, doc=doc_program_id),
        Argument("job_type", str, optional=True, default="container", doc=doc_job_type),
        Argument("template_cover_cmd_escape_bug", bool, optional=True, default=True, doc=doc_template_cover),
    ]

def lebesgue_executor_args():
    doc_extra = "The 'extra' key in the lebesgue executor. Note that we do not check if 'the `dict` provided to the 'extra' key is valid or not."
    return [
        Argument("extra", dict, lebesgue_extra_args(), optional = True, doc = doc_extra),
    ]

def dispatcher_args():
    """free style dispatcher args"""
    return []

def variant_executor():
    doc = f'The type of the executor.'
    return Variant("type", [
        Argument("lebesgue_v2", dict, lebesgue_executor_args()),
        Argument("dispatcher", dict, dispatcher_args()),
    ], doc = doc)

def template_conf_args():
    doc_image = 'The image to run the step.'
    doc_timeout = 'The time limit of the OP. Unit is second.'
    doc_retry_on_transient_error = 'The number of retry times if a TransientError is raised.'
    doc_timeout_as_transient_error = 'Treat the timeout as TransientError.'
    doc_envs = 'The environmental variables.'
    return [
        Argument("image", str, optional=True, default='dptechnology/dpgen2:latest', doc=doc_image),
        Argument("timeout", int, optional=True, default=None, doc=doc_timeout),
        Argument("retry_on_transient_error", int, optional=True, default=None, doc=doc_retry_on_transient_error),
        Argument("timeout_as_transient_error", bool, optional=True, default=False, doc=doc_timeout_as_transient_error),
        Argument("envs", dict, optional=True, default=None, doc=doc_envs),
    ]

def step_conf_args():
    doc_template = 'The configs passed to the PythonOPTemplate.'
    doc_executor = 'The executor of the step.'
    doc_continue_on_failed = 'If continue the the step is failed (FatalError, TransientError, A certain number of retrial is reached...).'
    doc_continue_on_num_success = 'Only in the sliced OP case. Continue the workflow if a certain number of the sliced jobs are successful.'
    doc_continue_on_success_ratio = 'Only in the sliced OP case. Continue the workflow if a certain ratio of the sliced jobs are successful.'
    doc_parallelism = 'The parallelism for the step'

    return [
        Argument("template_config", dict, template_conf_args(), optional=True, default={'image':'dptechnology/dpgen2:latest'}, doc=doc_template),
        Argument("continue_on_failed", bool, optional=True, default=False, doc=doc_continue_on_failed),
        Argument("continue_on_num_success", int, optional=True, default=None, doc=doc_continue_on_num_success),
        Argument("continue_on_success_ratio", float, optional=True, default=None, doc=doc_continue_on_success_ratio),
        Argument("parallelism", int, optional=True, default=None, doc=doc_parallelism),
        Argument("executor", dict, [], [variant_executor()], optional=True, default=None, doc = doc_executor),
    ]

def normalize_step_dict(data):
    sca = step_conf_args()
    base = Argument("base", dict, sca)
    data = base.normalize_value(data, trim_pattern="_*")
    # not possible to strictly check Lebesgue_executor_args, dirty hack!
    base.check_value(data, strict=False)
    return data

def init_executor(
        executor_dict,
):
    if executor_dict is None:
        return None
    etype = executor_dict.pop('type')
    if etype == "lebesgue_v2":
        return LebesgueExecutor(**executor_dict)
    if etype == "dispatcher":
        return DispatcherExecutor(**executor_dict)
    else:
        raise RuntimeError('unknown executor type', etype)

class PrepRunFp(Steps):
    def __init__(
        self,
        name : str,
        prep_op : OP,
        run_op : OP,
        prep_config : dict,
        run_config : dict,
        upload_python_packages : Optional[List[os.PathLike]] = None,
    ):
        self._input_parameters = {
            "config" : InputParameter(),
            "inputs" : InputParameter(),
            "optional_input" : InputParameter(),
            "type_map" : InputParameter(),
            "backward_list" : InputParameter(),
            "log_name" : InputParameter(type=str , value="log"),
            "backward_dir_name" : InputParameter(type=str , value="backward_dir"),
        }
        self._input_artifacts = {
            "confs" : InputArtifact(),
            "optional_artifact" : InputArtifact(), 
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
            prep_config = normalize_step_dict(prep_config),
            run_config = normalize_step_dict(run_config),
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
        prep_config : dict = normalize_step_dict({}),
        run_config : dict = normalize_step_dict({}),
        upload_python_packages : Optional[List[os.PathLike]] = None,
):
    prep_config = deepcopy(prep_config)
    run_config = deepcopy(run_config)
    try:
        command = run_config.pop('command')
    except:
        pass
    prep_template_config = prep_config.pop('template_config')
    run_template_config = run_config.pop('template_config')
    prep_executor = init_executor(prep_config.pop('executor'))
    run_executor = init_executor(run_config.pop('executor'))

    prep_fp = Step(
        'prep-fp' , 
        template=PythonOPTemplate(
            prep_op,
            output_artifact_archive={
                "task_paths": None
            },
            python_packages = upload_python_packages,
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
        executor = prep_executor,
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
        # with_param=argo_range(argo_len(prep_fp.outputs.parameters["task_names"])),
        key = step_keys['run-fp'],
        executor = run_executor,
        **run_config,
    )
    prep_run_steps.add(run_fp)

    prep_run_steps.outputs.artifacts["backward_dirs"]._from = run_fp.outputs.artifacts["backward_dir"]
