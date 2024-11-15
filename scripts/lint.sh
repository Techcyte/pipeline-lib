#/bin/bash
set -e .

EXIT_CODE=0

isort --check --diff pipeline_lib || EXIT_CODE=$?
black --diff --check pipeline_lib || EXIT_CODE=$?
mypy pipeline_lib || EXIT_CODE=$?
pylint pipeline_lib || EXIT_CODE=$?

exit $EXIT_CODE