from fpop.prep_fp import PrepFp
from fpop.run_fp import RunFp
import dpdata, sys, subprocess, os, shutil
from ase.io import read, write
from pathlib import Path
from dflow.utils import run_command
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
from dargs import (
    dargs, 
    Argument, 
    Variant, 
    ArgumentEncoder,
)
from dflow.python import (
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    TransientError,
    FatalError,
    BigParameter,
)

class Cp2kInputs:
    def __init__(self, inp_file: str):
        """
        Initialize the Cp2kInputs class.

        Parameters
        ----------
        inp_file : str
            The path to the user-submitted CP2K input file.
        """
        self.inp_file_from_file(inp_file)

    @property
    def inp_template(self):
        """
        Return the template content of the input file.
        """
        return self._inp_template

    def inp_file_from_file(self, fname: str):
        """
        Read the content of the input file and store it.

        Parameters
        ----------
        fname : str
            The path to the input file.
        """
        self._inp_template = Path(fname).read_text()

    @staticmethod
    def args():
        """
        Define the arguments required by the Cp2kInputs class.
        """
        doc_inp_file = "The path to the user-submitted CP2K input file."
        return [
            Argument("inp_file", str, optional=False, doc=doc_inp_file),
        ]

class PrepCp2k(PrepFp):
    def prep_task(
            self,
            conf_frame: dpdata.System,
            inputs: Cp2kInputs,
            prepare_image_config: Optional[Dict] = None,
            optional_input: Optional[Dict] = None,
            optional_artifact: Optional[Dict] = None,
    ):
        """
        Define how one CP2K task is prepared.

        Parameters
        ----------
        conf_frame : dpdata.System
            One frame of configuration in the dpdata format.
        inputs: Cp2kInputs
            The Cp2kInputs object handles the input file of the task.
        prepare_image_config: Dict, optional
            Definition of runtime parameters in the process of preparing tasks.
        optional_input: Dict, optional
            Other parameters the developers or users may need.
        optional_artifact: Dict[str, Path], optional
            Other files that users or developers need.
        """
        # Generate POSCAR file from the configuration frame
        conf_frame.to('vasp/poscar', 'POSCAR')

        # Read the structure from the POSCAR file and write to a temporary XYZ file
        atoms = read('POSCAR')
        write('temp.xyz', atoms)

        # Read the temporary XYZ file, remove the first two lines, and write to coord.xyz
        with open('temp.xyz', 'r') as f:
            lines = f.readlines()[2:]  # Remove the first two lines
        with open('coord.xyz', 'w') as f:
            f.writelines(lines)

        # Generate CELL_PARAMETER file
        cell_params = conf_frame['cells'][0]
        with open('CELL_PARAMETER', 'w') as file:
            file.write(f"A {cell_params[0,0]:14.8f} {cell_params[0,1]:14.8f} {cell_params[0,2]:14.8f}\n")
            file.write(f"B {cell_params[1,0]:14.8f} {cell_params[1,1]:14.8f} {cell_params[1,2]:14.8f}\n")
            file.write(f"C {cell_params[2,0]:14.8f} {cell_params[2,1]:14.8f} {cell_params[2,2]:14.8f}\n")

        # Write the CP2K input file content
        Path('input.inp').write_text(inputs.inp_template)

        # Copy optional files to the working directory
        if optional_artifact:
            for file_name, file_path in optional_artifact.items():
                content = file_path.read_text()
                Path(file_name).write_text(content)


class RunCp2k(RunFp):
    def input_files(self, task_path) -> List[str]:
        """
        The mandatory input files to run a CP2K task.
        
        Returns
        -------
        files: List[str]
            A list of mandatory input file names.
        """
        return ["input.inp", "CELL_PARAMETER", "coord.xyz"]

    def run_task(
        self,
        backward_dir_name,
        log_name,
        backward_list: List[str],
        run_image_config: Optional[Dict] = None,
        optional_input: Optional[Dict] = None,
    ) -> str:
        """
        Defines how one FP task runs.

        Parameters
        ----------
        backward_dir_name : str
            The name of the directory which contains the backward files.
        log_name : str
            The name of log file.
        backward_list : List[str]
            The output files the users need. For example: ["output.log", "trajectory.xyz"]
        run_image_config : Dict, optional
            Keyword args defined by the developer. For example:
            {
              "command": "source /opt/intel/oneapi/setvars.sh && mpirun -n 64 /opt/cp2k/bin/cp2k.popt"
            }
        optional_input : Dict, optional
            The parameters developers need in runtime. For example:
            {
                "conf_format": "cp2k/input"
            }
        
        Returns
        -------
        backward_dir_name : str
            The directory name which contains the files users need.
        """
        # Get the run command
        if run_image_config:
            command = run_image_config.get("command")
            if not command:
                raise ValueError("Command not specified in run_image_config")
        else:
            raise ValueError("run_image_config is missing")
        
        # Run CP2K command and write output to log file
        command = " ".join([command, ">", log_name])
        kwargs = {"try_bash": True, "shell": True}
        if run_image_config:
            kwargs.update(run_image_config)
            kwargs.pop("command", None)
        
        # Execute command
        ret, out, err = run_command(command, raise_error=False, **kwargs)  # type: ignore
        if ret != 0:
            raise TransientError(
                "cp2k failed\n", "out msg", out, "\n", "err msg", err, "\n"
            )
        
        # Check if the task was successful
        if not self.check_run_success(log_name):
            raise TransientError(
                "cp2k failed, we could not check the exact cause. Please check the log file."
            )
        
        # Create output directory and copy log file
        os.makedirs(Path(backward_dir_name))
        shutil.copyfile(log_name, Path(backward_dir_name) / log_name)
        for ii in backward_list:
            try:
                shutil.copyfile(ii, Path(backward_dir_name) / ii)
            except:
                shutil.copytree(ii, Path(backward_dir_name) / ii)
        
        return backward_dir_name
    
    def check_run_success(self, log_name):
        """
        Check if the CP2K task ran successfully by examining the output file.

        Returns
        -------
        success : bool
            True if the task ran successfully with warnings line, False otherwise.
        """
        with open(log_name, "r") as f:
            lines = f.readlines()
        return any("The number of warnings for this run is" in line for line in lines)
