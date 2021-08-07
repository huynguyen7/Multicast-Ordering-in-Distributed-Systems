#!/bin/sh

PYTHON_PATH=~/miniforge3/envs/mpi/bin/python3
mpiexec -np 5 $PYTHON_PATH ex.py
