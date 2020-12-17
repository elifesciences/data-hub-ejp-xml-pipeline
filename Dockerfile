FROM apache/airflow:1.10.13-python3.6
ARG install_dev=n

USER root

RUN apt-get update \
  && apt-get install sudo gcc -yqq \
  && rm -rf /var/lib/apt/lists/*

RUN usermod -aG sudo airflow
RUN echo "airflow ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers

USER airflow

COPY requirements.build.txt ./
RUN pip install --disable-pip-version-check -r requirements.build.txt --user

COPY requirements.txt ./
RUN pip install --disable-pip-version-check -r requirements.txt --user

USER airflow
COPY --chown=airflow:airflow requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install --disable-pip-version-check --user -r requirements.dev.txt; fi

ENV PATH /home/airflow/.local/bin:$PATH

COPY --chown=airflow:airflow ejp_xml_pipeline ./ejp_xml_pipeline
COPY --chown=airflow:airflow dags ./dags
COPY --chown=airflow:airflow setup.py ./setup.py
RUN pip install -e . --user --no-dependencies

COPY .pylintrc ./.pylintrc
COPY .flake8 ./.flake8
COPY --chown=airflow:airflow tests ./tests
COPY --chown=airflow:airflow run_test.sh ./
RUN if [ "${install_dev}" = "y" ]; then chmod +x run_test.sh; fi

COPY --chown=airflow:airflow worker.sh ./
RUN chmod +x worker.sh

RUN mkdir -p $AIRFLOW_HOME/serve
RUN ln -s $AIRFLOW_HOME/logs $AIRFLOW_HOME/serve/log

ENTRYPOINT []
