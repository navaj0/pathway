FROM continuumio/miniconda3

ENV PYTHONUNBUFFERED=TRUE
ENV PYTHONPATH ${PYTHONPATH}:/opt/ml/pathway

WORKDIR /opt/ml/pathway

COPY ./resources ./
RUN conda env create -f ./env.yml

# Make RUN commands use the new environment:
# See: https://pythonspeed.com/articles/activate-conda-dockerfile/
SHELL ["conda", "run", "-n", "default", "/bin/bash", "-c"]

COPY .pathway.tgz ./
RUN pip install .pathway.tgz

ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "default", "pathway-runtime"]