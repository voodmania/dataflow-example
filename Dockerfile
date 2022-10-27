FROM gcr.io/dataflow-templates-base/python39-template-launcher-base:latest

RUN pip install --upgrade pip

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# Due to a change in the Beam base image in version 2.24, we need to install
# libffi-dev manually as a dependency. For more information:
# https://github.com/GoogleCloudPlatform/python-docs-samples/issues/4891
RUN apt-get update -y && apt-get install -y libffi-dev git && rm -rf /var/lib/apt/lists/*

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"

RUN pip install -U google-cloud-bigquery==2.34.4 google-cloud-storage==2.5.0 apache-beam[gcp]==2.41.0

COPY --from=apache/beam_python3.9_sdk:2.41.0 /opt/apache/beam /opt/apache/beam
ENTRYPOINT ["/opt/apache/beam/boot"]

COPY utils/ ./utils/
COPY main.py .
