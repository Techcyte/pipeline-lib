FROM public.ecr.aws/docker/library/python:3.10.9-bullseye

# install pip
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3 get-pip.py
RUN rm get-pip.py

RUN mkdir pypkg
WORKDIR  /pypkg

COPY pipeline_executor/ ./pipeline_executor
COPY scripts/ ./scripts
COPY test/ ./test
COPY pyproject.toml ./

RUN pip install -e .[dev]

# disable python base image default entrypoint
ENTRYPOINT []
