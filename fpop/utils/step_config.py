import os
from dflow.config import config
from dflow.plugins.dispatcher import DispatcherExecutor

def init_executor(
    executor_dict,
):
    if executor_dict is None or config["mode"] == "debug":
        return None
    etype = executor_dict.pop("type")
    if etype == "dispatcher":
        return DispatcherExecutor(**executor_dict)
    else:
        raise RuntimeError("unknown executor type", etype)
