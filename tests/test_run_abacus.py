import unittest,os
from dflow.python import OPIO,TransientError
import shutil
from pathlib import Path
from mock import mock, patch, call
from context import fpop
from fpop.abacus import RunAbacus
from mocked_ops import MockedRunVasp
from constants import STRU1_content

class TestRunAbacus(unittest.TestCase):
    def setUp(self):

        self.cwd = os.getcwd()
        self.task_path = Path('abacustest')
        self.task_path.mkdir(parents=True, exist_ok=True)
        (self.task_path/"INPUT").write_text('INPUT_PARAMETERS\ncalculation scf\nbasis_type lcao\ndeepks_out_labels 1\n')
        (self.task_path/"KPT").write_text('here kpt')
        (self.task_path/"STRU").write_text(STRU1_content)
        (self.task_path/"Ga_ONCV_PBE-1.0.upf").write_text('here Ga_ONCV_PBE-1.0.upf')
        (self.task_path/"As_ONCV_PBE-1.0.upf").write_text('here As_ONCV_PBE-1.0.upf')
        (self.task_path/"Ga_gga_9au_100Ry_2s2p2d.orb").write_text('here Ga_gga_9au_100Ry_2s2p2d.orb')
        (self.task_path/"As_gga_8au_100Ry_2s2p1d.orb").write_text('here As_gga_8au_100Ry_2s2p1d.orb')
        (self.task_path/"jle.orb").write_text('here jle.orb')
        (self.task_path/'TEST1').write_text('here test1')
        (self.task_path/'TEST2').write_text('here test2')

        self.task_name = 'task_000'
        Path(self.task_name).mkdir(parents=True, exist_ok=True)
        (Path(self.task_name)/'our_log').write_text('here log')

    def tearDown(self):
        os.chdir(self.cwd)
        if os.path.isdir(self.task_path):
            shutil.rmtree(self.task_path)
        if os.path.isdir(self.task_name):
            shutil.rmtree(self.task_name)
    
    def test_inputfiles(self):
        op = RunAbacus()
        inputfiles = op.input_files(self.task_path)
        ref = ["INPUT","KPT","STRU","Ga_ONCV_PBE-1.0.upf","As_ONCV_PBE-1.0.upf","Ga_gga_9au_100Ry_2s2p2d.orb","As_gga_8au_100Ry_2s2p1d.orb","jle.orb"]
        ref.sort()
        inputfiles.sort()
        self.assertEqual(inputfiles,ref)


    @patch('fpop.abacus.run_command')
    def test_success(self, mocked_run):

        mocked_run.side_effect = [ (0, 'out\n', '') ]
        op = RunAbacus()
        def new_check_run_success(obj,logfile):
            return True
        with mock.patch.object(RunAbacus, "check_run_success", new=new_check_run_success):
            out = op.execute(
                OPIO({
                    'run_image_config' :{
                        'command' : 'myabacus',
                    },
                    'task_name' : self.task_name,
                    'task_path' : self.task_path,
                    'backward_list' : ['STRU','TEST1'],
                    'backward_dir_name' : 'our_backward',
                    'log_name' : 'our_log',
                    'optional_input' : {},
                    'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
                })
            )
        work_dir = Path(self.task_name)
        # check call
        calls = [
            call(' '.join(['myabacus', '>', 'our_log']), raise_error=False, try_bash=True),
        ]
        mocked_run.assert_has_calls(calls)

        # check output
        self.assertEqual(out["backward_dir"], work_dir/'our_backward')
        for ii in ['STRU','TEST1','our_log']:
            self.assertTrue(Path(Path(work_dir/'our_backward')/ ii).is_file())
        # check input files are correctly linked
        self.assertEqual((work_dir/'STRU').read_text(), (self.task_path/"STRU").read_text())
        self.assertEqual((work_dir/'INPUT').read_text(), (self.task_path/"INPUT").read_text())
        self.assertEqual((work_dir/'KPT').read_text(), 'here kpt')
        self.assertEqual((work_dir/'Ga_ONCV_PBE-1.0.upf').read_text(), 'here Ga_ONCV_PBE-1.0.upf')
        self.assertEqual((work_dir/'As_ONCV_PBE-1.0.upf').read_text(), 'here As_ONCV_PBE-1.0.upf')
        self.assertEqual((work_dir/'Ga_gga_9au_100Ry_2s2p2d.orb').read_text(), 'here Ga_gga_9au_100Ry_2s2p2d.orb')
        self.assertEqual((work_dir/'As_gga_8au_100Ry_2s2p1d.orb').read_text(), 'here As_gga_8au_100Ry_2s2p1d.orb')
        self.assertEqual((work_dir/'jle.orb').read_text(), 'here jle.orb')
        self.assertEqual((work_dir/'TEST1').read_text(), 'here test1')
        self.assertEqual((work_dir/'TEST2').read_text(), 'here test2')

    @patch('fpop.abacus.run_command')
    def test_error(self, mocked_run):
        mocked_run.side_effect = [ (1, 'out\n', '') ]
        op = RunAbacus()
        with self.assertRaises(TransientError) as ee:
            out = op.execute(
            OPIO({
                'run_image_config' :{
                    'command' : 'myabacus',
                },
                'task_name' : self.task_name,
                'task_path' : self.task_path,
                'backward_list' : ['STRU','TEST1'],
                'optional_input' : {},
                'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
            })
        )
        # check call
        calls = [
            call(' '.join(['myabacus', '>', 'log']), raise_error=False, try_bash=True),
        ]
        mocked_run.assert_has_calls(calls)

    @patch('fpop.abacus.run_command')
    def test_error_without_optional_parameter(self, mocked_run):
        mocked_run.side_effect = [ (1, 'out\n', '') ]
        op = RunAbacus()

        with self.assertRaises(TransientError) as ee:
            out = op.execute(
            OPIO({
                'run_image_config' :{
                    'command' : 'myabacus',
                },
                'task_name' : self.task_name,
                'task_path' : self.task_path,
                'backward_list' : ['STRU','TEST1'],
                'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
            })
        )
        # check call

        calls = [
            call(' '.join(['myabacus', '>', 'log']), raise_error=False, try_bash=True),
        ]
        mocked_run.assert_has_calls(calls)
