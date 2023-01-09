#/bin/bash
set -e .

export PYTHONDONTWRITEBYTECODE="set"
pytest --count=3 test -n 12