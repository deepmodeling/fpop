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

STRU1_content = """ATOMIC_SPECIES
Ga 69.723 Ga_ONCV_PBE-1.0.upf
As 74.922 As_ONCV_PBE-1.0.upf

NUMERICAL_ORBITAL
Ga_gga_9au_100Ry_2s2p2d.orb
As_gga_8au_100Ry_2s2p1d.orb

LATTICE_CONSTANT
1.889716

LATTICE_VECTORS
    5.75018     0.00000     0.00000
    0.00000     5.75018     0.00000
    0.00000     0.00000     5.75018

ATOMIC_POSITIONS
Direct

Ga
0.0
4
  0.0000000   0.0000000   0.0000000 1 1 1
  0.0000000   0.5000000   0.5000000 1 1 1
  0.5000000   0.0000000   0.5000000 1 1 1
  0.5000000   0.5000000   0.0000000 1 1 1

As
0.0
4
  0.2500000   0.2500000   0.2500000 1 1 1
  0.2500000   0.7500000   0.7500000 1 1 1
  0.7500000   0.2500000   0.7500000 1 1 1
  0.7500000   0.7500000   0.2500000 1 1 1

NUMERICAL_DESCRIPTOR
jle.orb
"""

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

