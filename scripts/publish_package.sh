#/bin/bash

# meant to be run from gitlab-ci step "publish-wheel"

set -e .

cd /pypkg
python -m build . --wheel
python -m twine upload --repository-url $REPOSITORY_URL dist/*.whl