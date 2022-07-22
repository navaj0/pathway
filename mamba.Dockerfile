FROM condaforge/mambaforge

RUN mamba update -y -n base mamba

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONPATH ${PYTHONPATH}:/opt/ml/pathway
ENV PACKAGEMANAGER=mamba

WORKDIR /opt/ml/pathway

COPY ./resources ./
RUN mamba env create -f ./env.yml

# Make RUN commands use the new environment:
# See: https://pythonspeed.com/articles/activate-conda-dockerfile/
SHELL ["mamba", "run", "-n", "default", "/bin/bash", "-c"]

COPY .pathway.tgz ./
RUN pip install .pathway.tgz

ENTRYPOINT ["mamba", "run", "--no-capture-output", "-n", "default", "pathway-runtime"]