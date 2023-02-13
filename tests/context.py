import sys,os
PrepRunFp_path = os.path.abspath(os.path.join(os.path.dirname(__file__),".."))
sys.path.insert(0,PrepRunFp_path)
import PrepRunFp
upload_python_packages = [os.path.join(PrepRunFp_path, 'PrepRunFp')]
if os.getenv('SKIP_UT_WITH_DFLOW'):
    if(os.getenv('SKIP_UT_WITH_DFLOW')!=0):
        skip_ut_with_dflow=1
    else:
        skip_ut_with_dflow=0
    skip_ut_with_dflow_reason = 'skip because environment variable SKIP_UT_WITH_DFLOW is set to non-zero'
else:
    skip_ut_with_dflow = False
    skip_ut_with_dflow_reason = ''
# one needs to set proper values for the following variable.
default_image = 'dptechnology/dpgen2:latest'
