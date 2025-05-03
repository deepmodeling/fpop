from context import fpop
import os,sys,json,glob,shutil,textwrap
import dpdata
import numpy as np
import unittest
from fpop.abacus import AbacusInputs,get_pporbdpks_from_stru
from pathlib import Path
from constants import STRU1_content

class TestAbacusInputs(unittest.TestCase):
    def setUp(self):
        self.task_path = Path('abacustest')
        self.work_path = Path('abacustest/work')
        self.work_path.mkdir(parents=True, exist_ok=True)
        (self.task_path/'INPUT').write_text('INPUT_PARAMETERS\ncalculation scf\nbasis_type pw\n')
        (self.task_path/'KPT').write_text('tkpt')
        (self.task_path/'H.upf').write_text('tH.upf')
        (self.task_path/'O.upf').write_text('tO.upf')
        (self.task_path/'H.orb').write_text('tH.orb')
        (self.task_path/'O.orb').write_text('tO.orb')
        (self.task_path/'jle.orb').write_text('tjle.orb')
        (self.task_path/'model.ptg').write_bytes(bytes("tmodel.ptg",encoding="utf-8"))

        self.cwd = os.getcwd()
        os.chdir(self.work_path)

    def tearDown(self):
        os.chdir(self.cwd)
        if Path(self.task_path).is_dir():
            shutil.rmtree(self.task_path)

    def test_writefile1(self):
        abacusinput = AbacusInputs(input_file="../INPUT",
                                   pp_files= {"H": "../H.upf", "O": "../O.upf"},
                                   orb_files= {"H": "../H.orb", "O": "../O.orb"},
                                   element_mass= {"H": 1, "O": 8},
                                   kpt_file="../KPT")
        abacusinput.write_input()
        abacusinput.write_kpt()
        abacusinput.write_pporb(["H"])
        
        self.assertTrue(os.path.isfile("INPUT"))
        self.assertTrue(os.path.isfile("KPT"))
        self.assertTrue(os.path.isfile("H.upf"))
        self.assertFalse(os.path.isfile("O.upf"))
        self.assertTrue(os.path.isfile("H.orb"))
        self.assertFalse(os.path.isfile("O.orb"))

        self.assertEqual("INPUT_PARAMETERS", Path("INPUT").read_text().split("\n")[0])
        self.assertTrue("calculation scf" in Path("INPUT").read_text().split("\n"))
        self.assertTrue("basis_type pw" in Path("INPUT").read_text().split("\n"))

        self.assertEqual(Path("KPT").read_text(),"tkpt")
        self.assertEqual(Path("H.upf").read_text(),"tH.upf")
        self.assertEqual(Path("H.orb").read_text(),"tH.orb")
    
    def test_writefile2(self):
        abacusinput = AbacusInputs(input_file="../INPUT",
                                   pp_files= {"H": "../H.upf", "O": "../O.upf"},
                                   orb_files= {"H": "../H.orb", "O": "../O.orb"},
                                   element_mass= {"H": 1, "O": 8},
                                   kpt_file="../KPT")
        abacusinput.write_input()
        abacusinput.write_kpt()
        abacusinput.write_pporb(["H","O"])
        
        self.assertTrue(os.path.isfile("INPUT"))
        self.assertTrue(os.path.isfile("KPT"))
        self.assertTrue(os.path.isfile("H.upf"))
        self.assertTrue(os.path.isfile("O.upf"))
        self.assertTrue(os.path.isfile("H.orb"))
        self.assertTrue(os.path.isfile("O.orb"))
    
    def test_writefile3(self):
        abacusinput = AbacusInputs(input_file="../INPUT",
                                   pp_files= {"H": "../H.upf", "O": "../O.upf"},
                                   orb_files= {"H": "../H.orb", "O": "../O.orb"},
                                   element_mass= {"H": 1, "O": 8},
                                   kpt_file="../KPT")
        abacusinput.set_input("basis_type","lcao")
        self.assertTrue(abacusinput.get_input()["basis_type"],"lcao")
        abacusinput.write_input()
        abacusinput.write_kpt()
        abacusinput.write_pporb(["H","O"])
        
        self.assertTrue(os.path.isfile("INPUT"))
        self.assertTrue(os.path.isfile("KPT"))

        self.assertTrue(os.path.isfile("H.upf"))
        self.assertTrue(os.path.isfile("O.upf"))
        self.assertTrue(os.path.isfile("H.orb"))
        self.assertTrue(os.path.isfile("O.orb"))

        self.assertEqual(abacusinput.get_input()["calculation"],"scf")
        self.assertEqual(abacusinput.get_input()["basis_type"],"lcao")
        self.assertEqual(abacusinput.get_mass(["H","O"]),[1,8])
        self.assertEqual(abacusinput.get_mass(["H","O","C"]),[1,8,12.0107])
        self.assertEqual(abacusinput.get_mass(["H","O","CCC"]),[1,8,1])

        self.assertEqual("INPUT_PARAMETERS", Path("INPUT").read_text().split("\n")[0])
        self.assertTrue("calculation scf" in Path("INPUT").read_text().split("\n"))
        self.assertTrue("basis_type lcao" in Path("INPUT").read_text().split("\n"))

        self.assertEqual(Path("KPT").read_text(),"tkpt")
        self.assertEqual(Path("H.upf").read_text(),"tH.upf")
        self.assertEqual(Path("H.orb").read_text(),"tH.orb")
        self.assertEqual(Path("O.upf").read_text(),"tO.upf")
        self.assertEqual(Path("O.orb").read_text(),"tO.orb")

    def test_setpporb(self):
        abacusinput = AbacusInputs(input_file="../INPUT",
                                   pp_files= {"H": "../H.upf", "O": "../O.upf"},
                                   orb_files= {"H": "../H.orb", "O": "../O.orb"},
                                   element_mass= {"H": 1, "O": 8},
                                   kpt_file="../KPT")
        self.assertEqual(abacusinput.get_pp()["H"][1],"tH.upf")
        self.assertEqual(abacusinput.get_pp()["O"][1],"tO.upf")
        self.assertEqual(abacusinput.get_orb()["H"][1],"tH.orb")
        self.assertEqual(abacusinput.get_orb()["O"][1],"tO.orb")

        abacusinput.set_pp("H","../O.orb")
        self.assertEqual(abacusinput.get_pp()["H"][1],"tO.orb")

        abacusinput.set_orb("H","../O.upf")
        self.assertEqual(abacusinput.get_orb()["H"][1],"tO.upf")
    
    def test_deepks(self):
        abacusinput = AbacusInputs(input_file="../INPUT",
                                   pp_files= {"H": "../H.upf", "O": "../O.upf"},
                                   orb_files= {"H": "../H.orb", "O": "../O.orb"},
                                   element_mass= {"H": 1, "O": 8},
                                   deepks_descriptor="../jle.orb",
                                   deepks_model="../model.ptg",
                                   kpt_file="../KPT")
        self.assertEqual(abacusinput.get_deepks_descriptor(),("jle.orb","tjle.orb"))
        self.assertEqual(abacusinput.get_deepks_model(),("model.ptg",bytes("tmodel.ptg",encoding="utf-8")))

        abacusinput.write_deepks()
        self.assertFalse(os.path.isfile("jle.orb"))
        self.assertFalse(os.path.isfile("model.ptg"))

        abacusinput.set_input("deepks_out_labels",1)
        abacusinput.write_deepks()
        self.assertTrue(os.path.isfile("jle.orb"))
        self.assertFalse(os.path.isfile("model.ptg"))

        abacusinput.set_input("deepks_scf",1)
        abacusinput.write_deepks()
        self.assertTrue(os.path.isfile("model.ptg"))

        self.assertEqual(Path("jle.orb").read_text(),"tjle.orb")
        self.assertEqual(Path("model.ptg").read_bytes(),bytes("tmodel.ptg",encoding="utf-8"))


class TestAbacusFunctions(unittest.TestCase):
    def setUp(self):
        self.work_path = Path('abacustest')
        self.work_path.mkdir(parents=True, exist_ok=True)
        (self.work_path/'STRU').write_text(STRU1_content)

        self.cwd = os.getcwd()
        os.chdir(self.work_path)

    def tearDown(self):
        os.chdir(self.cwd)
        if Path(self.work_path).is_dir():
            shutil.rmtree(self.work_path)
    
    def test_GetPporbdpksFromStru(self):
        stru_data = get_pporbdpks_from_stru("STRU")
        self.assertTrue(stru_data["pp"],["./Ga_ONCV_PBE-1.0.upf","./As_ONCV_PBE-1.0.upf"])
        self.assertTrue(stru_data["orb"],["./Ga_gga_9au_100Ry_2s2p2d.orb","./As_gga_8au_100Ry_2s2p1d.orb"])
        self.assertTrue(stru_data["dpks"],"jle.orb")
        self.assertTrue(stru_data["labels"],["Ga","As"])
        self.assertTrue(stru_data["mass"],[69.723,74.922])










        

        