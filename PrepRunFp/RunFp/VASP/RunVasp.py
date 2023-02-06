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
)
import numpy as np
import dpdata, sys, subprocess
from dargs import (
    dargs, 
    Argument, 
    Variant, 
    ArgumentEncoder,
)
from PrepRunFp.RunFp.RunFp import RunFp
from dflow.utils import run_command

class RunVasp(RunFp):
    def input_files(self) -> List[str]:
        r'''The mandatory input files to run a vasp task.
        Returns
        -------
        files: List[str]
            A list of madatory input files names.
        '''
        return ["POSCAR", "INCAR", "POTCAR", "KPOINTS"]

    def optional_input_files(self) -> List[str]:
        r'''The optional input files to run a vasp task.
        Returns
        -------
        files: List[str]
            A list of optional input files names.
        '''
        return []

    def run_task(
        self,
        command: str,
        out: str,
        log: str,
    ) -> Tuple[str, str]:
        r'''Defines how one FP task runs
        Parameters
        ----------
        command: str
            The command of running vasp task
        out: str
            The name of the output data file.
        log: str
            The name of the log file
        Returns
        -------
        out_name: str
            The file name of the output data in the dpdata.LabeledSystem format.
        log_name: str
            The file name of the log.
        '''

        log_name = log
        out_name = out
        # run vasp
        command = " ".join([command, ">", log_name])
        ret, out, err = run_command(command, raise_error=False, try_bash=True,)
        if ret != 0:
            raise TransientError(
                "vasp failed\n", "out msg", out, "\n", "err msg", err, "\n"
            )
        return log_name

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