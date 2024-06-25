# fpop
`fp` stands for first-principles calculation and `op` stands for operators. The abbreviation `fpop` stands for operators related to first-principles calculation.This project is based on [dflow](https://github.com/deepmodeling/dflow) which is a Python framework for constructing scientific computing workflows (e.g. concurrent learning workflows) employing Argo Workflows as the workflow engine. 
# Installation
```
pip install fpop
```
# Develop Guide
If you want to support a new first-principles computing software in `fpop`, you can refer to the writeup of `fpop/vasp.py` and `fpop/abacus.py`.
Specifically, you need to rewrite the abstract method `prep_task` of class `PrepFp`, and rewrite `run_task` and `input_files`(if needed) methods of class `RunFp`.
