import sys
from pathlib import Path

MYPY = "https://github.com/python/mypy.git"
MYPY_EXE = Path(sys.executable).parent / "mypy"
LOCAL_MYPY = Path(".tox/typecheck/mypy")

DOIT_CONFIG = {"default_tasks": ["typecheck"]}


def task_typecheck():
    return {
        "file_dep": [MYPY_EXE],
        "uptodate": [False],
        "actions": [f"{sys.executable} -m mypy %(posargs)s"],
        "pos_arg": "posargs",
        "verbosity": 2,
    }


def task_install_mypy():
    return {
        "file_dep": [LOCAL_MYPY / "setup.py"],
        "targets": [MYPY_EXE],
        "uptodate": [MYPY_EXE.exists],
        "actions": [f"{sys.executable} -m pip install -e {LOCAL_MYPY}"],
        "verbosity": 2,
    }


def task_clone_mypy():
    return {
        "targets": [LOCAL_MYPY / "setup.py"],
        "uptodate": [LOCAL_MYPY.is_dir],
        "actions": [
            f"git clone --recurse-submodules {MYPY} {LOCAL_MYPY}",
            f"cd {LOCAL_MYPY} && git submodule update",
        ],
        "verbosity": 2,
    }
