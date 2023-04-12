from mocked_ops import TestInputFiles, TestInputFiles2
from pathlib import Path
import unittest
import shutil

class TestRunInputFiles(unittest.TestCase):
    def setUp(self):
        self.task_path = Path('task')
        self.task_path.mkdir(parents=True, exist_ok=True)
        (self.task_path/'123').write_text('123')
        (self.task_path/'efg').write_text('efg')
        (self.task_path/'oo').write_text('oo')
    
    def tearDown(self):
        if Path('task').is_dir():
            shutil.rmtree('task')
    
    def test_default(self):
        fake = TestInputFiles()
        result = fake.input_files('task')
        ref = ['123', 'oo', 'efg']
        ref.sort()
        result.sort()
        self.assertEqual(result,ref)

    def test_rewrite(self):
        fake = TestInputFiles2()
        result = fake.input_files('task')
        ref = ["abc","ee"]
        ref.sort()
        result.sort()
        self.assertEqual(result,ref)
