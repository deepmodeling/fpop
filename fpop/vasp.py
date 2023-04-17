from fpop.prep_fp import PrepFp
from fpop.run_fp import RunFp
import dpdata, sys, subprocess, os, shutil
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

class VaspInputs():
    def __init__(
            self,
            kspacing : Union[float, List[float]],
            incar : str,
            pp_files : Dict[str, str],
            kgamma : bool = True,
    ):
        """
        Parameters
        ----------
        kspacing : Union[float, List[float]]
                The kspacing. If it is a number, then three directions use the same
                ksapcing, otherwise it is a list of three numbers, specifying the
                kspacing used in the x, y and z dimension.
        incar: str
                A template INCAR file. 
        pp_files : Dict[str,str]
                The potcar files for the elements. For example
                { 
                   "H" : "/path/to/POTCAR_H",
                   "O" : "/path/to/POTCAR_O",
                }
        kgamma : bool
                K-mesh includes the gamma point
        """
        self.kspacing = kspacing
        self.kgamma = kgamma
        self.incar_from_file(incar)
        self.potcars_from_file(pp_files)

    @property
    def incar_template(self):
        return self._incar_template

    @property
    def potcars(self):
        return self._potcars

    def incar_from_file(
            self,
            fname : str,
    ):
        self._incar_template = Path(fname).read_text()

    def potcars_from_file(
            self,
            dict_fnames : Dict[str,str],
    ):
        self._potcars = {}
        for kk,vv in dict_fnames.items():
            self._potcars[kk] = Path(vv).read_text()            

    def make_potcar(
            self, 
            atom_names,
    ) -> str:        
        potcar_contents = []
        for nn in atom_names:
            potcar_contents.append(self._potcars[nn])
        return "".join(potcar_contents)            

    def make_kpoints(
            self,
            box : np.ndarray,
    ) -> str:
        return make_kspacing_kpoints(box, self.kspacing, self.kgamma)

    @staticmethod
    def args():
        doc_pp_files = 'The pseudopotential files set by a dict, e.g. {"Al" : "path/to/the/al/pp/file", "Mg" : "path/to/the/mg/pp/file"}'
        doc_incar = "The path to the template incar file"
        doc_kspacing = "The spacing of k-point sampling. `ksapcing` will overwrite the incar template"
        doc_kgamma = "If the k-mesh includes the gamma point. `kgamma` will overwrite the incar template"
        return [
            Argument("incar", str, optional=False, doc=doc_pp_files),
            Argument("pp_files", dict, optional=False, doc=doc_pp_files),
            Argument("kspacing", float, optional=False, doc=doc_kspacing),
            Argument("kgamma", bool, optional=True, default=True, doc=doc_kgamma),
        ]

def make_kspacing_kpoints(box, kspacing, kgamma) :
    if type(kspacing) is not list:
        kspacing = [kspacing, kspacing, kspacing]
    box = np.array(box)
    rbox = _reciprocal_box(box)
    kpoints = [max(1,(np.ceil(2 * np.pi * np.linalg.norm(ii) / ks).astype(int))) for ii,ks in zip(rbox,kspacing)]
    ret = _make_vasp_kpoints(kpoints, kgamma)
    return ret


def _make_vasp_kp_gamma(kpoints):
    ret = ""
    ret += "Automatic mesh\n"
    ret += "0\n"
    ret += "Gamma\n"
    ret += "%d %d %d\n" % (kpoints[0], kpoints[1], kpoints[2])
    ret += "0  0  0\n"
    return ret

def _make_vasp_kp_mp(kpoints):
    ret = ""
    ret += "K-Points\n"
    ret += "0\n"
    ret += "Monkhorst Pack\n"
    ret += "%d %d %d\n" % (kpoints[0], kpoints[1], kpoints[2])
    ret += "0  0  0\n"
    return ret

def _make_vasp_kpoints (kpoints, kgamma = False) :
    if kgamma :
        ret = _make_vasp_kp_gamma(kpoints)
    else :
        ret = _make_vasp_kp_mp(kpoints)
    return ret
    
def _reciprocal_box(box) :
    rbox = np.linalg.inv(box)
    rbox = rbox.T
    return rbox


class PrepVasp(PrepFp):
    def prep_task(
            self,
            conf_frame: dpdata.System,
            vasp_inputs: VaspInputs,
            prepare_image_config: Optional[Dict] = None,
            optional_input: Optional[Dict] = None,
            optional_artifact: Optional[Dict] = None,
    ):
        r"""Define how one Vasp task is prepared.

        Parameters
        ----------
        conf_frame : dpdata.System
            One frame of configuration in the dpdata format.
        inputs: VaspInputs
            The VaspInputs object handels all other input files of the task.
        prepare_image_config: Dict
            Definition of runtime parameters in the process of preparing tasks. 
        optional_input: 
            Other parameters the developers or users may need.For example:
            {
               "oonf_format": "vasp/poscar"
            }
            optional_input["conf_format"]: The format of the configurations which users give.  
        optional_artifact
            Other files that users or developers need.For example:
            {
               "ICONST": Path("./iconst")
            }
            In vasp part, all the files which are given in optional_artifact will be copied to the work directory. In this example, "INCAR","POTCAR","POSCAR","KPOINTS" and "ICONST" will be copied to the same directory. "./iconst" is the path where the target file exists.
        """

        conf_frame.to('vasp/poscar', 'POSCAR')
        Path('INCAR').write_text(
            vasp_inputs.incar_template
        )
        # fix the case when some element have 0 atom, e.g. H0O2
        tmp_frame = dpdata.System('POSCAR', fmt='vasp/poscar')
        Path('POTCAR').write_text(
            vasp_inputs.make_potcar(tmp_frame['atom_names'])
        )
        Path('KPOINTS').write_text(
            vasp_inputs.make_kpoints(conf_frame['cells'][0])
        )

        if optional_artifact:
            for file_name, file_path in optional_artifact.items():
                content = file_path.read_text()
                Path(file_name).write_text(content)


class RunVasp(RunFp):
    def input_files(self, task_path) -> List[str]:
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
            The output files the users need.For example: ["OUTCAR","CONTCAR"]
        run_image_config:
            Keyword args defined by the developer.For example:
            {
              "command": "source /opt/intel/oneapi/setvars.sh && mpirun -n 64 /opt/vasp.5.4.4/bin/vasp_std"
            }
        optional_input:
            The parameters developers need in runtime.For example:
            {
                "conf_format": "vasp/poscar"
            }
            optional_input["vasp/poscar"] is the format of the configurations that users give.
        
        Returns
        -------
        backward_dir_name: str
            The directory name which containers the files users need.
        '''
        if run_image_config:
            command = run_image_config["command"]
        else:
            command = "vasp_std"
        # run vasp
        command = " ".join([command, ">", log_name])
        ret, out, err = run_command(command, raise_error=False, try_bash=True,)
        if ret != 0:
            raise TransientError(
                "vasp failed\n", "out msg", out, "\n", "err msg", err, "\n"
            )
        if not self.check_run_success():
            raise TransientError(
                "vasp failed , we could not check the exact cause . Please check log file ."
            )
        os.makedirs(Path(backward_dir_name))
        shutil.copyfile(log_name,Path(backward_dir_name)/log_name)
        for ii in backward_list:
            shutil.copyfile(ii,Path(backward_dir_name)/ii)
        return backward_dir_name
    
    def check_run_success(self):
        with open("OUTCAR","r") as f:
            lines = f.readlines()
        if "Voluntary" in lines[-1]:
            return True
        else:
            return False
