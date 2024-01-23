FROM apache/airflow:2.8.1-python3.8
ARG install_dev=n

USER root

RUN sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 467B942D3A79BD29

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
COPY requirements.dev.txt ./
RUN if [ "${install_dev}" = "y" ]; then pip install --disable-pip-version-check --user -r requirements.dev.txt; fi

ENV PATH /home/airflow/.local/bin:$PATH

COPY ejp_xml_pipeline ./ejp_xml_pipeline
COPY dags ./dags
COPY setup.py ./setup.py
RUN pip install -e . --user --no-dependencies

COPY .pylintrc ./.pylintrc
COPY .flake8 ./.flake8
COPY tests ./tests
COPY mypy.ini ./
COPY run_test.sh ./

RUN mkdir -p $AIRFLOW_HOME/serve
RUN ln -s $AIRFLOW_HOME/logs $AIRFLOW_HOME/serve/log

ENTRYPOINT []
