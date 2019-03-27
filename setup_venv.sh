#!/bin/bash
set -e

rm -rf venv
virtualenv venv
venv/bin/pip install --no-deps -r requirements.txt

pip install --upgrade pip
