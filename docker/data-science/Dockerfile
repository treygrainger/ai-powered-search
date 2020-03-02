FROM jupyter/scipy-notebook

USER root

# Spark dependencies
ENV APACHE_SPARK_VERSION=2.4.5 \
    HADOOP_VERSION=2.7 \
    SPARK_SOLR_VERSION=3.7.3

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
    rm -rf /var/lib/apt/lists/*

# Using the preferred mirror to download the file
RUN cd /tmp && \
    wget -q $(wget -qO- https://www.apache.org/dyn/closer.lua/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz\?as_json | \
    python -c "import sys, json; content=json.load(sys.stdin); print(content['preferred']+content['path_info'])") && \
    echo "2426a20c548bdfc07df288cd1d18d1da6b3189d0b78dee76fa034c52a4e02895f0ad460720c526f163ba63a17efae4764c46a1cd8f9b04c60f9937a554db85d2 *spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | sha512sum -c - && \
    tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner && \
    rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark


# Spark Config
ENV SPARK_HOME=/usr/local/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip \
    SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" \
    PATH=$PATH:$SPARK_HOME/bin

# Install Spark-Solr
Run echo $SPARK_HOME
Run cd /tmp && \
    wget -q https://repo1.maven.org/maven2/com/lucidworks/spark/spark-solr/${SPARK_SOLR_VERSION}/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar && \
    $SPARK_HOME/bin/spark-shell --jars spark-solr-${SPARK_SOLR_VERSION}-shaded.jar && \
    rm spark-solr-${SPARK_SOLR_VERSION}-shaded.jar

# python -c "import sys, json; content=json.load(sys.stdin); print(content['preferred']+content['path_info'])") && \
#    echo "f42d79f96aad12c880d47cc772043ae84e73d5595093206b6b7ae5818faadc1bcda21c57c09c9dd3436d68ebf61d791199cfc1bb8969367fc04e8309933df7b3 *spark-solr-${SPARK_SOLR_VERSION}-shaded.jar" | sha512sum -c - && \


USER $NB_UID

# Install pyarrow
RUN conda install --quiet -y 'pyarrow' && \
    conda clean --all -f -y && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

# Install tensorflow
#RUN pip install 'tensorflow==2.1.0' && \
#    fix-permissions $CONDA_DIR && \
#    fix-permissions /home/$NB_USER

# Setup an AI-Powered Search user
#RUN useradd -ms /bin/bash aips
WORKDIR ~

# Pull Requirements, Install Notebooks
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY notebooks ~/notebooks/
WORKDIR ~/notebooks

#todo: require password for security. Turning off in first release to avoid initial complexity for readers.
CMD jupyter notebook --ip=0.0.0.0 --no-browser --NotebookApp.token='' --NotebookApp.password=''