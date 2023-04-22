from abc import ABC,abstractmethod
from dflow.utils import set_directory
from dflow.python import (
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    TransientError,
    FatalError,
    BigParameter,
    Parameter,
)
from dflow import (
    Workflow,
    Step,
    upload_artifact,
    download_artifact,
    InputArtifact,
    OutputArtifact,
    ShellOPTemplate
)
import os, json, shutil
from pathlib import Path
from typing import (
    Any,
    Tuple,
    List,
    Set,
    Dict,
    Optional,
    Union,
)
import numpy as np

class RunFp(OP, ABC):
    r'''Execute a first-principles (FP) task.
    A working directory named `task_name` is created. All input files
    are copied or symbol linked to directory `task_name`. The FP
    command is exectuted from directory `task_name`. 
    '''

    @classmethod
    def get_input_sign(cls):
        return OPIOSign(
            {
                "task_name": str,
                "task_path": Artifact(Path),
                "backward_list": List[str],
                "log_name": Parameter(str,default='log'),
                "backward_dir_name": Parameter(str,default='backward_dir'),
                "run_image_config": BigParameter(dict,default={}),
                "optional_artifact": Artifact(Dict[str,Path],optional=True),
                "optional_input": BigParameter(dict,default={})
            }
        )

    @classmethod
    def get_output_sign(cls):
        return OPIOSign(
            {
                "backward_dir": Artifact(Path),
            }
        )

    def input_files(self, task_path) -> List[str]:
        r'''The mandatory input files to run a FP task.
        Returns
        -------
        files: List[str]
            A list of madatory input files names.
        '''
        return os.listdir(task_path)

    @abstractmethod
    def run_task(
        self,
        backward_dir_name,
        log_name,
        backward_list: List[str],
        run_image_config: Optional[Dict]=None,
        optional_input: Optional[Dict]=None,
    ) -> str:
        r'''Defines how one FP task runs
        Parameters
        ----------
        backward_dir_name:
            The name of the directory which contains the backward files.
        log_name:
            The name of log file.
        backward_list:
            The output files the users need.
        run_image_config:
            Keyword args defined by the developer.
        optional_input:
            The parameters developers need in runtime.For example:
                                {
                                  "conf_format": "vasp/poscar"
                                }
                                optional_input["vasp/poscar"] is the format of the configurations that users give.
                                Other keys in optional_input are defined by different developers.
        
        Returns
        -------
        backward_dir_name: str
            The directory name which containers the files users need.
        '''
        pass

    @OP.exec_sign_check
    def execute(
        self,
        ip: OPIO,
    ) -> OPIO:
        r'''Execute the OP.
        Parameters
        ----------
        ip : dict
            Input dict with components:
            - `task_name`: (`str`) The name of task.
            - `task_path`: (`Artifact(Path)`) The path that contains all input files prepareed by `PrepFp`.
            - `backward_list`: (`List[str]`) The output files the users need.
            - `log_name`: (`str`) The name of log file.
            - `backward_dir_name`: (`str`) The name of the directory which contains the backward files.
            - `run_image_config`: (`dict`) It defines the runtime configuration of the FP task.
            - `optional_artifact` : (`Artifact(Dict[str,Path])`) Other files that users or developers need.Other files that users or developers need.The using method of this part are defined by different developers.For example, in vasp part, all the files which are given in optional_artifact will be copied to the working directory.
            - `optional_input` : (`dict`) Other parameters the developers or users may need.For example:
                                {
                                  "conf_format": "vasp/poscar"
                                }
                                optional_input["vasp/poscar"] is the format of the configurations that users give.
                                Other keys in optional_input are defined by different developers.
        Returns
        -------
            Output dict with components:
            - `backward_dir`: (`Artifact(Path)`) The directory which contains the files users need.
        Exceptions
        ----------
        TransientError
            On the failure of FP execution.
        FatalError
            When mandatory files are not found.
        '''
        run_image_config = ip["run_image_config"]
        backward_dir_name = ip["backward_dir_name"] 
        log_name = ip["log_name"] 
        backward_list = ip["backward_list"]
        optional_input = ip["optional_input"]
        task_name = ip["task_name"]
        task_path = ip["task_path"]
        input_files = self.input_files(task_path)
        input_files = [(Path(task_path) / ii).resolve() for ii in input_files]
        work_dir = Path(task_name)
        opt_input_files = []
        if ip["optional_artifact"]:
            for ss,vv in ip["optional_artifact"].items():
                opt_input_files.append(ss)
        opt_input_files = [(Path(task_path) / ii).resolve() for ii in opt_input_files]

        with set_directory(work_dir,mkdir=True):
            # link input files
            for ii in input_files:
                if not os.path.exists(ii):
                    raise FatalError(f"cannot file file/directory {ii}")
                iname = ii.name
                Path(iname).symlink_to(ii)
            for ii in opt_input_files:
                if os.path.exists(ii):
                    iname = ii.name
                    Path(iname).symlink_to(ii)
            backward_dir_name = self.run_task(backward_dir_name,log_name,backward_list,run_image_config,optional_input)

        return OPIO(
            {
                "backward_dir": work_dir / backward_dir_name
            }
        )
