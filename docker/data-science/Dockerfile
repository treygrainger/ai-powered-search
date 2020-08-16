# THis tag uses python 3.7, pyspark does not work with python 3.8 except on
# Spark 3.0 (see https://github.com/apache/spark/pull/26194)
FROM jupyter/scipy-notebook:619e9cc2fc07

USER root

# Spark dependencies
ENV APACHE_SPARK_VERSION=2.4.6 \
    HADOOP_VERSION=2.7 \
    SPARK_SOLR_VERSION=3.8.0

RUN apt-get -y update && \
    apt-get install --no-install-recommends -y openjdk-8-jre-headless ca-certificates-java && \
    rm -rf /var/lib/apt/lists/*

# Using the preferred mirror to download the file
RUN cd /tmp && \
    wget -q $(wget -qO- https://www.apache.org/dyn/closer.lua/spark/spark-${APACHE_SPARK_VERSION}/spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz\?as_json | \
    python -c "import sys, json; content=json.load(sys.stdin); print(content['preferred']+content['path_info'])") && \
    echo "3a9f401eda9b5749cdafd246b1d14219229c26387017791c345a23a65782fb8b25a302bf4ac1ed7c16a1fe83108e94e55dad9639a51c751d81c8c0534a4a9641 *spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" | sha512sum -c - && \
    tar xzf spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /usr/local --owner root --group root --no-same-owner && \
    rm spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
RUN cd /usr/local && ln -s spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark


# Spark Config
ENV SPARK_HOME=/usr/local/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip \
    SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --spark.driver.extraLibraryPath=/usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar --spark.executor.extraLibraryPath=/usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar --driver-java-options=-Dlog4j.logLevel=info" \
    PATH=$PATH:$SPARK_HOME/bin
ENV SPARK_CLASSPATH=$SPARK_CLASSPATH:/usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar
ENV PYSPARK_SUBMIT_ARGS="--jars /usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar"

# Install Spark-Solr
Run echo $SPARK_HOME
Run mkdir /usr/local/spark/lib/ && cd /usr/local/spark/lib/ && \
    wget -q https://repo1.maven.org/maven2/com/lucidworks/spark/spark-solr/${SPARK_SOLR_VERSION}/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar && \
    echo "3bd0614d50ce6ef2769eb0d654e58fd68cf3e1f63c567dca8b12432a7e6ac907753b289f6d3cca5a80a67454d6ff841e438f53472cba37530293548751edaa8f *spark-solr-${SPARK_SOLR_VERSION}-shaded.jar" | sha512sum -c - && \
    export EXTRA_CLASSPATH=/usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar && \
    $SPARK_HOME/bin/spark-shell --jars spark-solr-${SPARK_SOLR_VERSION}-shaded.jar
    #&& \ rm spark-solr-${SPARK_SOLR_VERSION}-shaded.jar

Run chmod a+rwx /usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar

# python -c "import sys, json; content=json.load(sys.stdin); print(content['preferred']+content['path_info'])") && \
#    echo "f42d79f96aad12c880d47cc772043ae84e73d5595093206b6b7ae5818faadc1bcda21c57c09c9dd3436d68ebf61d791199cfc1bb8969367fc04e8309933df7b3 *spark-solr-${SPARK_SOLR_VERSION}-shaded.jar" | sha512sum -c - && \

COPY notebooks notebooks
RUN chown -R $NB_UID:$NB_UID /home/$NB_USER

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

WORKDIR /home/$NB_USER

# Pull Requirements, Install Notebooks
COPY requirements.txt ./
RUN pip install -r requirements.txt

#RUN git clone https://github.com/ai-powered-search/retrotech.git $WORKDIR/retrotech

#COPY notebooks notebooks

#RUN chown -R $NB_UID:$NB_UID /home/$NB_USER
#RUN fix-permissions /home/$NB_USER
#RUN fix-permissions notebooks

WORKDIR notebooks
# If you want to edit the notebooks and have
# your changes persist, uncomment the line below
# and restart with `docker-compose up --build`
# WORKDIR /tmp/notebooks


#todo: require password for security. Turning off in first release to avoid initial complexity for readers.
CMD jupyter notebook --ip=0.0.0.0 --no-browser --NotebookApp.token='' --NotebookApp.password=''
