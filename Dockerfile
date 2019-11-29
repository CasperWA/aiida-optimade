FROM python:3.7

ENV AIIDA_PATH /app

WORKDIR /app

RUN git clone https://github.com/Materials-Consortia/optimade-python-tools
RUN pip install -e optimade-python-tools[mongo]
RUN pip install uvicorn

# install AiiDA version
# can be deleted once v1.0.1 is released
RUN git clone https://github.com/aiidateam/aiida-core
ARG AIIDA_VERSION=v1.0.0
RUN git -C aiida-core checkout $AIIDA_VERSION
RUN pip install -e aiida-core

# copy repo contents
COPY setup.py ./
COPY aiida_optimade ./aiida_optimade
RUN pip install -e .

# copy AiiDA configuration
COPY .docker/run.sh ./

EXPOSE 80

CMD ["/app/run.sh"]
