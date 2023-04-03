from typing import List
from pathlib import Path
import dpdata,os
POSCAR_1_content='''Na bcc
   1.0
    -1.3917984866239606    1.3917984866239606    1.3917984866239606
     1.3917984866239606   -1.3917984866239606    1.3917984866239606
     1.3917984866239606    1.3917984866239606   -1.3917984866239606
Na
   1
Direct
  0.0000000000000000  0.0000000000000000  0.0000000000000000
'''

POSCAR_2_content='''Na fcc
   1.0
     0.0000000000000000    1.7146885303020956    1.7146885303020956
     1.7146885303020956    0.0000000000000000    1.7146885303020956
     1.7146885303020956    1.7146885303020956    0.0000000000000000
Na
   1
Direct
  0.5000000000000000  0.5000000000000000  0.5000000000000000
'''

def dump_conf_from_poscar(
        type,
        conf_list
        ) -> List[str] :
    for ii in range(len(conf_list)):
        Path("POSCAR_%d"%ii).write_text(conf_list[ii])
    if type == "deepmd/npy":
        confs = []
        for ii in range(len(conf_list)):
            ls = dpdata.System("POSCAR_%d"%ii, fmt="vasp/poscar")
            ls.to_deepmd_npy("data.%03d"%ii)
            confs.append("data.%03d"%ii)
            os.remove("POSCAR_%d"%ii)
        return confs
    elif type == "vasp/poscar":
        confs = []
        for ii in range(len(conf_list)):
            confs.append("POSCAR_%d")
        return confs
    else:
        return []

