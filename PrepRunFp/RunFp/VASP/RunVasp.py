from dflow.python import (
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    TransientError,
    FatalError,
    BigParameter,
)
from typing import (
    Tuple,
    List,
    Optional,
    Dict,
)
import numpy as np
import dpdata, sys, subprocess, os, shutil
from dargs import (
    dargs, 
    Argument, 
    Variant, 
    ArgumentEncoder,
)
from PrepRunFp.RunFp.RunFp import RunFp
from dflow.utils import run_command
from pathlib import Path

class RunVasp(RunFp):
    def input_files(self) -> List[str]:
        r'''The mandatory input files to run a vasp task.
        Returns
        -------
        files: List[str]
            A list of madatory input files names.
        '''
        return ["POSCAR", "INCAR", "POTCAR", "KPOINTS"]

    def run_task(
        self,
        backward_dir_name,
        log_name,
        backward_list: List[str],
        run_config: Optional[Dict]=None,
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
        run_config:
            Keyword args defined by the developer.
            The fp/run_config session of the input file will be passed to this function.
        optional_input:
            The parameters developers need in runtime.
        
        Returns
        -------
        backward_dir_name: str
            The directory name which containers the files users need.
        '''
        if run_config:
            command = run_config["command"]
        else:
            command = "vasp_std"
        # run vasp
        command = " ".join([command, ">", log_name])
        ret, out, err = run_command(command, raise_error=False, try_bash=True,)
        if ret != 0:
            raise TransientError(
                "vasp failed\n", "out msg", out, "\n", "err msg", err, "\n"
            )
        os.makedirs(Path(backward_dir_name))
        shutil.copyfile(log_name,Path(backward_dir_name)/log_name)
        for ii in backward_list:
            shutil.copyfile(ii,Path(backward_dir_name)/ii)
        return backward_dir_name

    @staticmethod
    def args():
        r'''The argument definition of the `run_task` method.
        Returns
        -------
        arguments: List[dargs.Argument]
            List of dargs.Argument defines the arguments of `run_task` method.
        '''

        doc_vasp_cmd = "The command of VASP"
        doc_vasp_log = "The log file name of VASP"
        doc_vasp_out = "The output dir name of labeled data. In `deepmd/npy` format provided by `dpdata`."
        return [
            Argument("command", str, optional=True, default="vasp", doc=doc_vasp_cmd),
            Argument(
                "out",
                str,
                optional=True,
                default="data",
                doc=doc_vasp_out,
            ),
            Argument(
                "log", str, optional=True, default="fp.log", doc=doc_vasp_log
            ),
        ]
