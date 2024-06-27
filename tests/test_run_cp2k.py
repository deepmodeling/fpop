import unittest,os
from dflow.python import OPIO,TransientError
import shutil
from pathlib import Path
from mock import mock, patch, call
from context import fpop
from fpop.cp2k import RunCp2k


class TestRunCp2k(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.task_path = Path('task/path')
        self.task_path.mkdir(parents=True, exist_ok=True)
        (self.task_path / 'input.inp').write_text('here input.inp')
        (self.task_path / 'CELL_PARAMETER').write_text('here CELL_PARAMETER')
        (self.task_path / 'coord.xyz').write_text('here coord.xyz')
        (self.task_path / 'TEST1').write_text('here test1')
        (self.task_path / 'TEST2').write_text('here test2')
        self.task_name = 'task_000'
        Path(self.task_name).mkdir(parents=True, exist_ok=True)
        (Path(self.task_name)/'our_log').write_text('here log')

    def tearDown(self):
        os.chdir(self.cwd)
        if Path('task').is_dir():
            shutil.rmtree('task')
        if Path(self.task_name).is_dir():
            shutil.rmtree(self.task_name)
    
    @patch('fpop.cp2k.run_command')
    def test_success(self, mocked_run):
        mocked_run.side_effect = [ (0, 'out\n', '') ]
        op = RunCp2k()
        def new_check_run_success(obj, log_name):
            return True
        with mock.patch.object(RunCp2k, "check_run_success", new=new_check_run_success):
            out = op.execute(
                OPIO({
                    'run_image_config' :{
                        'command' : 'myCp2k',
                    },
                    'task_name' : self.task_name,
                    'task_path' : self.task_path,
                    'backward_list' : ['input.inp','TEST1'],
                    'backward_dir_name' : 'our_backward',
                    'log_name' : 'our_log',
                    'optional_input' : {},
                    'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
                })
            )
        work_dir = Path(self.task_name)
        # check call
        calls = [
            call(' '.join(['myCp2k', '>', 'our_log']), raise_error=False, try_bash=True, shell=True),
        ]
        mocked_run.assert_has_calls(calls)
        # check output
        self.assertEqual(out["backward_dir"], work_dir/'our_backward')
        for ii in ['input.inp', 'TEST1', 'our_log']:
            self.assertTrue(Path(Path(work_dir/'our_backward')/ ii).is_file())
        # check input files are correctly linked
        self.assertEqual((work_dir / 'input.inp').read_text(), 'here input.inp')
        self.assertEqual((work_dir / 'CELL_PARAMETER').read_text(), 'here CELL_PARAMETER')
        self.assertEqual((work_dir / 'coord.xyz').read_text(), 'here coord.xyz')
        self.assertEqual((work_dir / 'TEST1').read_text(), 'here test1')
        self.assertEqual((work_dir / 'TEST2').read_text(), 'here test2')

    @patch('fpop.cp2k.run_command')
    def test_success_without_optional_parameter(self, mocked_run):
        mocked_run.side_effect = [ (0, 'out\n', '') ]
        op = RunCp2k()
        def new_check_run_success(obj, log_name):
            return True
        with mock.patch.object(RunCp2k, "check_run_success", new=new_check_run_success):
            out = op.execute(
                OPIO({
                    'run_image_config' :{
                        'command' : 'myCp2k',
                    },
                    'task_name' : self.task_name,
                    'task_path' : self.task_path,
                    'backward_list' : ['input.inp', 'TEST1'],
                    'backward_dir_name' : 'our_backward',
                    'log_name' : 'our_log',
                    'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
                })
            )
        work_dir = Path(self.task_name)
        # check call
        calls = [
            call(' '.join(['myCp2k', '>', 'our_log']), raise_error=False, try_bash=True, shell=True),
        ]
        mocked_run.assert_has_calls(calls)
        # check output
        self.assertEqual(out["backward_dir"], work_dir/'our_backward')
        for ii in ['input.inp','TEST1','our_log']:
            self.assertTrue(Path(Path(work_dir/'our_backward')/ ii).is_file())
        # check input files are correctly linked
        self.assertEqual((work_dir / 'input.inp').read_text(), 'here input.inp')
        self.assertEqual((work_dir / 'CELL_PARAMETER').read_text(), 'here CELL_PARAMETER')
        self.assertEqual((work_dir / 'coord.xyz').read_text(), 'here coord.xyz')
        self.assertEqual((work_dir / 'TEST1').read_text(), 'here test1')
        self.assertEqual((work_dir / 'TEST2').read_text(), 'here test2')

    @patch('fpop.cp2k.run_command')
    def test_error(self, mocked_run):
        mocked_run.side_effect = [ (1, 'out\n', '') ]
        op = RunCp2k()
        with self.assertRaises(TransientError) as ee:
            out = op.execute(
            OPIO({
                'run_image_config' :{
                    'command' : 'myCp2k',
                },
                'task_name' : self.task_name,
                'task_path' : self.task_path,
                'backward_list' : ['input.inp', 'TEST1'],
                'optional_input' : {},
                'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
            })
        )
        # check call
        calls = [
            call(' '.join(['myCp2k', '>', 'log']), raise_error=False, try_bash=True, shell=True),
        ]
        mocked_run.assert_has_calls(calls)

    @patch('fpop.cp2k.run_command')
    def test_error_without_optional_parameter(self, mocked_run):
        mocked_run.side_effect = [ (1, 'out\n', '') ]
        op = RunCp2k()
        with self.assertRaises(TransientError) as ee:
            out = op.execute(
            OPIO({
                'run_image_config' :{
                    'command' : 'myCp2k',
                },
                'task_name' : self.task_name,
                'task_path' : self.task_path,
                'backward_list' : ['input.inp', 'TEST1'],
                'optional_artifact' : {'TEST1':Path(''),'TEST2':Path('')},
            })
        )
        # check call
        calls = [
            call(' '.join(['myCp2k', '>', 'log']), raise_error=False, try_bash=True, shell=True),
        ]
        mocked_run.assert_has_calls(calls)
