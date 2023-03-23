import sys,os
fpop_path = os.path.abspath(os.path.join(os.path.dirname(__file__),".."))
sys.path.insert(0,fpop_path)
import fpop
upload_python_packages = [os.path.join(fpop_path, 'fpop')]
if os.getenv('SKIP_UT_WITH_DFLOW'):
    if(os.getenv('SKIP_UT_WITH_DFLOW')=='0'):
        skip_ut_with_dflow=0
    else:
        skip_ut_with_dflow=1
    skip_ut_with_dflow_reason = 'skip because environment variable SKIP_UT_WITH_DFLOW is set to non-zero'
else:
    skip_ut_with_dflow = False
    skip_ut_with_dflow_reason = ''
# one needs to set proper values for the following variable.
default_image = 'dptechnology/dpgen2:latest'
if os.getenv("DFLOW_DEBUG"):
    from dflow.config import config
    config["mode"] = "debug"
