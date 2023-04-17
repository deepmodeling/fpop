import unittest,os
from dflow.python import OPIO,TransientError
import shutil
from pathlib import Path
from mock import mock, patch, call
from context import fpop
from fpop.vasp import RunVasp
from mocked_ops import MockedRunVasp

class TestRunVasp(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.task_path = Path('task/path')
        self.task_path.mkdir(parents=True, exist_ok=True)
        (self.task_path/'POSCAR').write_text('here poscar')
        (self.task_path/'INCAR').write_text('here incar')
        (self.task_path/'POTCAR').write_text('here potcar')
        (self.task_path/'KPOINTS').write_text('here kpoints')
        (self.task_path/'TEST1').write_text('here test1')
        (self.task_path/'TEST2').write_text('here test2')
        self.task_name = 'task_000'
        Path(self.task_name).mkdir(parents=True, exist_ok=True)
        (Path(self.task_name)/'our_log').write_text('here log')

    def tearDown(self):
        os.chdir(self.cwd)
        if Path('task').is_dir():
            shutil.rmtree('task')
        if Path(self.task_name).is_dir():
            shutil.rmtree(self.task_name)
    
    @patch('fpop.vasp.run_command')
    def test_success(self, mocked_run):
        mocked_run.side_effect = [ (0, 'out\n', '') ]
        op = RunVasp()
        def new_check_run_success(obj):
            return True
        with mock.patch.object(RunVasp, "check_run_success", new=new_check_run_success):
            out = op.execute(
                OPIO({
                    'run_image_config' :{
                        'command' : 'myvasp',
                    },
                    'task_name' : self.task_name,
                    'task_path' : self.task_path,
                    'backward_list' : ['POSCAR','TEST1'],
                    'backward_dir_name' : 'our_backward',
                    'log_name' : 'our_log',
                    'optional_input' : {},
                    'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
                })
            )
        work_dir = Path(self.task_name)
        # check call
        calls = [
            call(' '.join(['myvasp', '>', 'our_log']), raise_error=False, try_bash=True),
        ]
        mocked_run.assert_has_calls(calls)
        # check output
        self.assertEqual(out["backward_dir"], work_dir/'our_backward')
        for ii in ['POSCAR','TEST1','our_log']:
            self.assertTrue(Path(Path(work_dir/'our_backward')/ ii).is_file())
        # check input files are correctly linked
        self.assertEqual((work_dir/'POSCAR').read_text(), 'here poscar')
        self.assertEqual((work_dir/'INCAR').read_text(), 'here incar')
        self.assertEqual((work_dir/'POTCAR').read_text(), 'here potcar')
        self.assertEqual((work_dir/'KPOINTS').read_text(), 'here kpoints')
        self.assertEqual((work_dir/'TEST1').read_text(), 'here test1')
        self.assertEqual((work_dir/'TEST2').read_text(), 'here test2')

    @patch('fpop.vasp.run_command')
    def test_success_without_optional_parameter(self, mocked_run):
        mocked_run.side_effect = [ (0, 'out\n', '') ]
        op = RunVasp()
        def new_check_run_success(obj):
            return True
        with mock.patch.object(RunVasp, "check_run_success", new=new_check_run_success):
            out = op.execute(
                OPIO({
                    'run_image_config' :{
                        'command' : 'myvasp',
                    },
                    'task_name' : self.task_name,
                    'task_path' : self.task_path,
                    'backward_list' : ['POSCAR','TEST1'],
                    'backward_dir_name' : 'our_backward',
                    'log_name' : 'our_log',
                    'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
                })
            )
        work_dir = Path(self.task_name)
        # check call
        calls = [
            call(' '.join(['myvasp', '>', 'our_log']), raise_error=False, try_bash=True),
        ]
        mocked_run.assert_has_calls(calls)
        # check output
        self.assertEqual(out["backward_dir"], work_dir/'our_backward')
        for ii in ['POSCAR','TEST1','our_log']:
            self.assertTrue(Path(Path(work_dir/'our_backward')/ ii).is_file())
        # check input files are correctly linked
        self.assertEqual((work_dir/'POSCAR').read_text(), 'here poscar')
        self.assertEqual((work_dir/'INCAR').read_text(), 'here incar')
        self.assertEqual((work_dir/'POTCAR').read_text(), 'here potcar')
        self.assertEqual((work_dir/'KPOINTS').read_text(), 'here kpoints')
        self.assertEqual((work_dir/'TEST1').read_text(), 'here test1')
        self.assertEqual((work_dir/'TEST2').read_text(), 'here test2')

    @patch('fpop.vasp.run_command')
    def test_error(self, mocked_run):
        mocked_run.side_effect = [ (1, 'out\n', '') ]
        op = RunVasp()
        with self.assertRaises(TransientError) as ee:
            out = op.execute(
            OPIO({
                'run_image_config' :{
                    'command' : 'myvasp',
                },
                'task_name' : self.task_name,
                'task_path' : self.task_path,
                'backward_list' : ['POSCAR','TEST1'],
                'optional_input' : {},
                'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
            })
        )
        # check call
        calls = [
            call(' '.join(['myvasp', '>', 'log']), raise_error=False, try_bash=True),
        ]
        mocked_run.assert_has_calls(calls)

    @patch('fpop.vasp.run_command')
    def test_error_without_optional_parameter(self, mocked_run):
        mocked_run.side_effect = [ (1, 'out\n', '') ]
        op = RunVasp()
        with self.assertRaises(TransientError) as ee:
            out = op.execute(
            OPIO({
                'run_image_config' :{
                    'command' : 'myvasp',
                },
                'task_name' : self.task_name,
                'task_path' : self.task_path,
                'backward_list' : ['POSCAR','TEST1'],
                'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
            })
        )
        # check call
        calls = [
            call(' '.join(['myvasp', '>', 'log']), raise_error=False, try_bash=True),
        ]
        mocked_run.assert_has_calls(calls)







