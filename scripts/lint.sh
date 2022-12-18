#/bin/bash
set -e .

EXIT_CODE=0

isort --check-only . || EXIT_CODE=$?
black --check . || EXIT_CODE=$?
mypy pipeline_executor || EXIT_CODE=$?
pylint pipeline_executor || EXIT_CODE=$?

exit $EXIT_CODE