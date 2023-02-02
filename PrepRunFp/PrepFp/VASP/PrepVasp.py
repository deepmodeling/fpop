from PrepRunFp.PrepFp.PrepFp import PrepFp
from PrepRunFp.PrepFp.VASP.VaspInputs import VaspInputs
import dpdata
from pathlib import Path

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
