from abc import ABC,abstractmethod
import os
from pathlib import Path
from dflow.utils import set_directory
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    TransientError,
    FatalError,
    BigParameter,
)
from typing import (
    Any,
    Tuple,
    List,
    Set,
    Dict,
    Optional,
    Union,
)
import dpdata

class PrepFp(OP, ABC):
    r"""Prepares the working directories for first-principles (FP) tasks.

    A list of (same length as ip["confs"]) working directories
    containing all files needed to start FP tasks will be
    created. The paths of the directories will be returned as
    `op["task_paths"]`. The identities of the tasks are returned as
    `op["task_names"]`.

    """

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            "inputs" : BigParameter(object),
            "type_map": List[str],
            "confs" : Artifact(List[Path]),
            "prep_image_config" : BigParameter(dict,default={}),
            "optional_input" : BigParameter(dict,default={}),
            "optional_artifact" : Artifact(Dict[str,Path],optional=True)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            "task_names": List[str],
            "task_paths" : Artifact(List[Path]),
        })

    @abstractmethod
    def prep_task(
            self,
            conf_frame,
            inputs: Any,
            prepare_image_config: Optional[Dict] = None,
            optional_input: Optional[Dict] = None,
            optional_artifact: Optional[Dict] = None,
    ):
        r"""Define how one FP task is prepared.

        Parameters
        ----------
        conf_frame : dpdata.System
            One frame of configuration in the dpdata format.
        inputs: Any
            The class object handels all other input files of the task. 
            For example, pseudopotential file, k-point file and so on.
        prepare_image_config: Dict
            Definition of runtime parameters in the process of preparing tasks. 
        optional_input: 
            Other parameters the developers or users may need.For example:
            {
               "conf_format": "vasp/poscar"
            }
            optional_input["vasp/poscar"] is the format of the configurations that users give.
            Other keys in optional_input are defined by different developers.
        optional_artifact:
            Other files that users or developers need.The using method of this part are defined by different developers.For example, in vasp part, all the files which are given in optional_artifact will be copied to the working directory.
        """
        pass

    @OP.exec_sign_check
    def execute(
            self,
            ip : OPIO,
    ) -> OPIO:
        r"""Execute the OP.

        Parameters
        ----------
        ip : dict
            Input dict with components:

            - `prep_image_config` : (`dict`) It defines the parameters of the process of preparing FP tasks.
            - `inputs` : (`object`) The class object handels all other input files of the task. For example, pseudopotential file, k-point file and so on.
            - `type_map` : (`List[str]`) The list of elements.
            - `confs` : (`Artifact(List[Path])`) Configurations for the FP tasks. Stored in folders as formats which can be read by dpdata.System. 
                The format can be defined by parameter "conf_format" in "optional_input". The default format is deepmd/npy. 
            - `optional_input` : (`dict`) Other parameters the developers or users may need.For example:
                                {
                                  "conf_format": "vasp/poscar"
                                }
                                optional_input["vasp/poscar"] is the format of the configurations that users give.
                                Other keys in optional_input are defined by different developers.
            - `optional_artifact` : (` Artifact(Dict[str,Path])`) Other files that users or developers need.The using method of this part are defined by different developers.For example, in vasp part, all the files which are given in optional_artifact will be copied to the working directory.

        Returns
        -------
        op : dict 
            Output dict with components:

            - `task_names`: (`List[str]`) The name of tasks. Will be used as the identities of the tasks. The names of different tasks are different.
            - `task_paths`: (`Artifact(List[Path])`) The parepared working paths of the tasks. Contains all input files needed to start the FP. The order fo the Paths should be consistent with `op["task_names"]`
        """
        

        inputs = ip['inputs']
        confs = ip['confs']
        type_map = ip['type_map']
        prepare_image_config = ip["prep_image_config"]
        optional_artifact = ip["optional_artifact"]
        optional_input = ip["optional_input"]
        try:
            conf_format = optional_input["conf_format"]
        except:
            conf_format = "deepmd/npy"

        task_names = []
        task_paths = []

        #System
        counter = 0
        # loop over list of System
        self._register_spin()
        for system in confs:
            ss = dpdata.System(system, fmt=conf_format, labeled=False)
            for ff in range(ss.get_nframes()):
                nn, pp = self._exec_one_frame(counter, inputs, ss[ff], prepare_image_config, optional_input, optional_artifact)
                task_names.append(nn)
                task_paths.append(pp)
                counter += 1

        return OPIO({
            'task_names' : task_names,
            'task_paths' : task_paths,
        })

    def _register_spin(self):
        from dpdata.data_type import Axis, DataType
        import numpy as np
        dt = DataType(
            "spins",
            np.ndarray,
            (Axis.NFRAMES, Axis.NATOMS, 3),
            required=False,
            deepmd_name="spin",
        )
        dpdata.System.register_data_type(dt)

    def _exec_one_frame(
            self,
            idx,
            inputs,
            conf_frame,
            prepare_image_config = None,
            optional_input = None,
            optional_artifact = None,
    ) -> Tuple[str, Path]:
        task_name = 'task.' + '%06d' % idx
        task_path = Path(task_name)
        with set_directory(task_path,mkdir=True):
            self.prep_task(conf_frame, inputs, prepare_image_config, optional_input, optional_artifact)
        return task_name, task_path
