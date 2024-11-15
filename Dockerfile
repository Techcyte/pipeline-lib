FROM public.ecr.aws/docker/library/python:3.10.9-bullseye@sha256:60d76c989e429c4af605ebc17806b319a0f265d833564f3e2a61fc7a23e810e8

# install pip
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3 get-pip.py
RUN rm get-pip.py

RUN mkdir pypkg
WORKDIR  /pypkg

COPY pipeline_lib/ ./pipeline_lib
COPY scripts/ ./scripts
COPY test/ ./test
COPY pyproject.toml mypy.ini ./

RUN pip install -e .[dev]

# disable python base image default entrypoint
ENTRYPOINT []
