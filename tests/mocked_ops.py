from dflow.python import (
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    upload_packages,
    FatalError,
)

upload_packages.append(__file__)

import os, json, shutil, re, pickle, glob
from pathlib import Path
from typing import Tuple, List, Optional, Dict
from context import fpop
from fpop.vasp import RunVasp
from fpop.run_fp import RunFp

class MockedRunVasp(RunVasp):
    @OP.exec_sign_check
    def execute(
            self,
            ip : OPIO,
    ) -> OPIO:
        task_name = ip["task_name"]
        task_path = ip["task_path"]

        assert(ip['task_path'].is_dir())
        assert(re.match('task.[0-9][0-9][0-9][0-9][0-9][0-9]', ip['task_name']))
        assert(ip['task_name'] in str(ip['task_path']))
        for ii in ['INCAR','POSCAR','POTCAR','KPOINTS']:
            assert((ip['task_path']/ii).is_file())

        work_dir = Path(task_name)

        cwd = os.getcwd()
        work_dir.mkdir(exist_ok=True, parents=True)
        os.chdir(work_dir)

        ifiles = glob.glob(str(task_path/'*'))
        for ii in ifiles:
            if not Path(Path(ii).name).exists():
                Path(Path(ii).name).symlink_to(ii)

        backward_dir = Path(ip['backward_dir_name'])
        backward_dir.mkdir(exist_ok=True, parents=True)
        log = Path(Path(ip["backward_dir_name"])/ip["log_name"])

        fc=[]
        for ii in ['POSCAR','INCAR']:
            fc.append(Path(ii).read_text())
        log.write_text('\n'.join(fc))

        for ii in ip["backward_list"]:
            fc = []
            fc.append(f'This is {ii} which users need in {task_name}')
            fc.append(Path('KPOINTS').read_text())
            (backward_dir / ii).write_text('\n'.join(fc))
    
        os.chdir(cwd)

        return OPIO({
            'backward_dir' : work_dir / backward_dir
        })


class TestInputFiles(RunFp):
    def run_task(
        self,
        backward_dir_name,
        log_name,
        backward_list: List[str],
        run_image_config: Optional[Dict]=None,
        optional_input: Optional[Dict]=None,
    ):
        pass

class TestInputFiles2(RunFp):
    def input_files(self, task_path) -> List[str]:
        return ["abc","ee"]

    def run_task(
        self,
        backward_dir_name,
        log_name,
        backward_list: List[str],
        run_image_config: Optional[Dict]=None,
        optional_input: Optional[Dict]=None,
    ):
        pass
