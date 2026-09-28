# Imagem Airflow da plataforma — sem cluster Spark.
# Plugins de sistema necessarios: Oracle Instant Client (fontes Oracle).
ARG AIRFLOW_VERSION=3.2.2
FROM apache/airflow:${AIRFLOW_VERSION}

ARG AIRFLOW_VERSION

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    libaio1 \
    libaio-dev \
    unzip \
    pkg-config \
    apt-utils && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Oracle Instant Client (zips em plugin/, ignorados pelo Git)
ARG ORACLE_IC_VERSION=23.26.1.0.0

RUN mkdir -p /opt/oracle/instantclient \
    && mkdir -p /opt/airflow/config \
    && mkdir -p /opt/airflow/scripts \
    && mkdir -p /usr/local/airflow \
    && ln -sf /usr/share/zoneinfo/America/Sao_Paulo /etc/localtime

ADD plugin/instantclient-basic-linux.x64-${ORACLE_IC_VERSION}.zip /opt/oracle/
ADD plugin/instantclient-sdk-linux.x64-${ORACLE_IC_VERSION}.zip /opt/oracle/

RUN unzip -o /opt/oracle/instantclient-basic-linux.x64-${ORACLE_IC_VERSION}.zip -d /opt/oracle && \
    unzip -o /opt/oracle/instantclient-sdk-linux.x64-${ORACLE_IC_VERSION}.zip -d /opt/oracle && \
    IC_DIR=$(ls -d /opt/oracle/instantclient_* 2>/dev/null | head -1) && \
    if [ -n "$IC_DIR" ] && [ -d "$IC_DIR" ]; then \
        mv ${IC_DIR}/* /opt/oracle/instantclient/ && \
        rm -rf ${IC_DIR}; \
    fi && \
    mkdir -p /opt/oracle/instantclient/lib && \
    find /opt/oracle/instantclient -maxdepth 1 -name "*.so*" -exec cp {} /opt/oracle/instantclient/lib/ \; && \
    find /opt/oracle/instantclient -maxdepth 1 -name "*.jar" -exec cp {} /opt/oracle/instantclient/lib/ \;

ENV ORACLE_HOME=/opt/oracle/instantclient
ENV LD_LIBRARY_PATH=/opt/oracle/instantclient
ENV PATH=$ORACLE_HOME:$PATH
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow/config:/opt/airflow/libs

RUN echo "$ORACLE_HOME" > /etc/ld.so.conf.d/oracle.conf && ldconfig

USER airflow

COPY requirements.txt /requirements.txt
COPY requirements-jobs.txt /requirements-jobs.txt
COPY sitecustomize.py /opt/airflow/config/sitecustomize.py

RUN pip install --upgrade pip setuptools && \
    pip install kiwisolver --prefer-binary && \
    PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')" && \
    pip install --no-cache-dir -r /requirements.txt \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
        --use-feature=fast-deps

# Overlay dos jobs (sem constraints): deltalake / pyarrow / boto3
RUN pip install --no-cache-dir -r /requirements-jobs.txt

USER root

RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/config /opt/airflow/libs /usr/local/airflow/logs

COPY airflow.cfg /opt/airflow/config/airflow.cfg

RUN chown -R airflow:0 /opt/airflow/config /opt/airflow/scripts \
    && chmod -R 775 /opt/airflow/config /opt/airflow/scripts \
    && chmod -R 775 /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /usr/local/airflow/logs

USER airflow

CMD ["airflow", "api-server"]
