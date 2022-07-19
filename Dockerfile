FROM python:3.8-slim-buster

ENV PYTHONUNBUFFERED=TRUE

RUN pip3 install boto3 scikit-learn pandas smart_open

ENV PYTHONPATH ${PYTHONPATH}:/opt/ml/pathway

WORKDIR /opt/ml/pathway
COPY ./src ./setup.py ./
RUN pip3 install ./

ENTRYPOINT ["pathway-runtime"]