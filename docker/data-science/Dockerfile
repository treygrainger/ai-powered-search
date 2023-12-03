FROM jupyter/pyspark-notebook:spark-3.3.1

USER root

#install gcc, c++, and related dependencies needed to for pip to build some python dependencies
RUN sudo apt-get -y update && apt-get install -y --reinstall build-essential gcc cargo

# Spark dependencies
ENV SPARK_SOLR_VERSION=4.0.2
ENV SHADED_SOLR_JAR_PATH=/usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar

# Install Spark-Solr
RUN mkdir -p /usr/local/spark/lib/ && cd /usr/local/spark/lib/ && \
    wget -q https://repo1.maven.org/maven2/com/lucidworks/spark/spark-solr/${SPARK_SOLR_VERSION}/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar -O $SHADED_SOLR_JAR_PATH && \
    echo "c5293f10257603bcf650780afcb91ed1bb118f09feb731502c2dc7ac14ba950e586a033cb2f50e5c122c5ec442dc0d2b55f76c4f6522b555e67f4981a38bca26 *spark-solr-${SPARK_SOLR_VERSION}-shaded.jar" | sha512sum -c - && \
    chmod a+rwx $SHADED_SOLR_JAR_PATH

COPY notebooks notebooks

WORKDIR /home/$NB_USER

# Pull Requirements, Install Notebooks
COPY requirements.txt ./
ENV BLIS_ARCH="generic" 

RUN python -m pip --no-cache-dir install --upgrade pip && \
  pip --no-cache-dir install -r requirements.txt

RUN python -m spacy download en_core_web_sm
RUN pip --no-cache-dir install https://github.com/explosion/spacy-experimental/releases/download/v0.6.1/en_coreference_web_trf-3.4.0a2-py3-none-any.whl

COPY log4j.properties /usr/local/spark/conf/

RUN chown -R $NB_UID:$NB_UID /home/$NB_USER
USER $NB_UID

# Spark Config
ENV SPARK_OPTS="$SPARK_OPTS --driver-java-options=\"-DXlint:none -Dlog4j.logLevel=error -Dallow-access=java.nio.DirectByteBuffer -Dlog4j.logger.org.apache.spark.repl.Main=ERROR\" --spark.ui.showConsoleProgress=False --spark.driver.extraLibraryPath=$SHADED_SOLR_JAR_PATH --spark.executor.extraLibraryPath=$SHADED_SOLR_JAR_PATH" \
    PYSPARK_SUBMIT_ARGS="-c spark.driver.defaultJavaOptions=\"-DXlint=none -Dlog4j.logLevel=error -Dallow-access=java.nio.DirectByteBuffer\" -c spark.ui.showConsoleProgress=False --jars $SHADED_SOLR_JAR_PATH pyspark-shell" \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*-src.zip:%PYTHONPATH% \
    DOCKER_STACKS_JUPYTER_CMD=lab

WORKDIR notebooks
# If you want to edit the notebooks and have
# your changes persist, uncomment the line below
# and restart with `docker-compose up --build`

#WORKDIR /tmp/notebooks

# Mark all notebooks as trusted by default
RUN find . -name \*.ipynb -print0|xargs -0 jupyter-trust -y

CMD start-notebook.sh --ServerApp.token='' --ServerApp.password='' --ServerApp.ip=0.0.0.0 --no-browser \
  --LabApp.default_url='/lab/tree/welcome.ipynb'
