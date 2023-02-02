import sys
sys.path.append("..")
from PrepFp import PrepFp
from VaspInputs import VaspInputs
import dpdata

class PrepVasp(PrepFp):
    def prep_task(
            self,
            conf_frame: dpdata.System,
            vasp_inputs: VaspInputs,
    ):
        r"""Define how one Vasp task is prepared.

        Parameters
        ----------
        conf_frame : dpdata.System
            One frame of configuration in the dpdata format.
        inputs: VaspInputs
            The VaspInputs object handels all other input files of the task.
        """

        conf_frame.to('vasp/poscar', vasp_conf_name)
        Path(vasp_input_name).write_text(
            vasp_inputs.incar_template
        )
        # fix the case when some element have 0 atom, e.g. H0O2
        tmp_frame = dpdata.System(vasp_conf_name, fmt='vasp/poscar')
        Path(vasp_pot_name).write_text(
            vasp_inputs.make_potcar(tmp_frame['atom_names'])
        )
        Path(vasp_kp_name).write_text(
            vasp_inputs.make_kpoints(conf_frame['cells'][0])
        )
