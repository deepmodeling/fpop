import sys
sys.path.append("..")
from RunFp import RunFp
from typing import (
    Tuple,
    List,
    Optional,
)

class RunVasp(RunFp):
    def input_files(self) -> List[str]:
        r'''The mandatory input files to run a vasp task.
        Returns
        -------
        files: List[str]
            A list of madatory input files names.
        '''
        return [vasp_conf_name, vasp_input_name, vasp_pot_name, vasp_kp_name]

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
        ret, out, err = run_command(command, shell=True)
        if ret != 0:
            raise TransientError(
                "vasp failed\n", "out msg", out, "\n", "err msg", err, "\n"
            )
        # convert the output to deepmd/npy format
        sys = dpdata.LabeledSystem("OUTCAR")
        sys.to("deepmd/npy", out_name)
        return out_name, log_name

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
                default=fp_default_out_data_name,
                doc=doc_vasp_out,
            ),
            Argument(
                "log", str, optional=True, default=fp_default_log_name, doc=doc_vasp_log
            ),
        ]