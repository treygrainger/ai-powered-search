# syntax = docker/dockerfile:1.5
FROM jupyter/pyspark-notebook:spark-3.3.1
USER root

# Install gcc, c++, and related dependencies needed for pip to build some python dependencies
RUN sudo apt-get -y update && \
    apt-get install -y --reinstall build-essential gcc cargo && \
    rm -rf /var/lib/apt/lists/ 

# Install Spark-Solr
ENV SPARK_SOLR_VERSION=4.0.2
ENV SHADED_SOLR_JAR_PATH=/usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar
RUN mkdir -p /usr/local/spark/lib/ && cd /usr/local/spark/lib/ && \
    wget -q https://repo1.maven.org/maven2/com/lucidworks/spark/spark-solr/${SPARK_SOLR_VERSION}/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar -O $SHADED_SOLR_JAR_PATH && \
    echo "c5293f10257603bcf650780afcb91ed1bb118f09feb731502c2dc7ac14ba950e586a033cb2f50e5c122c5ec442dc0d2b55f76c4f6522b555e67f4981a38bca26 *spark-solr-${SPARK_SOLR_VERSION}-shaded.jar" \
    | sha512sum -c - && chmod 0777 $SHADED_SOLR_JAR_PATH

# Install Spark-OpenSearch
ENV SPARK_OS_VERSION=1.2.0
ENV SPARK_OS_JAR=opensearch-spark-30_2.12-${SPARK_OS_VERSION}.jar
ENV SPARK_OS_PATH=/usr/local/spark/lib/${SPARK_OS_JAR}
RUN cd /usr/local/spark/lib/ && \
    wget -q https://repo1.maven.org/maven2/org/opensearch/client/opensearch-spark-30_2.12/${SPARK_OS_VERSION}/${SPARK_OS_JAR} -O $SPARK_OS_PATH && \
    echo "5b9ae056b6ac21ae009f79a3a761774c7178b995fbe035572a4f35d5738e055d02828d2ec0ff98dd063ffffe37f4c48dc9a418d71269fc560f65b33c94493f2d *${SPARK_OS_JAR}" \
    | sha512sum -c - && chmod 0777 $SPARK_OS_PATH

WORKDIR /home/$NB_USER

# Install Python dependencies
ENV BLIS_ARCH="generic" PIP_CACHE_DIR=/var/cache/pip
RUN mkdir -p $PIP_CACHE_DIR
COPY build/ch5_spacy_requirements.txt /home/$NB_USER
RUN conda create --name ch5-spacy python=3.10.0 -y
RUN --mount=type=cache,target=$PIP_CACHE_DIR \
    conda run --name ch5-spacy pip install -r ch5_spacy_requirements.txt
RUN conda run --name ch5-spacy python -m spacy link en_core_web_sm en_core_web_sm
RUN conda run --name ch5-spacy python -m ipykernel install --name ch5-spacy --display-name "[ONLY FOR CH5.1] spaCy experimental kernel"

COPY build/requirements.txt /home/$NB_USER
RUN --mount=type=cache,target=$PIP_CACHE_DIR \
    pip install -r requirements.txt && \
    python -m spacy download en_core_web_sm
RUN rm ch5_spacy_requirements.txt requirements.txt

# Configure home directory
COPY build/log4j.properties /usr/local/spark/conf/
COPY aips/ /home/$NB_USER/aips
COPY chapters/ /home/$NB_USER/chapters
COPY course/ /home/$NB_USER/course
COPY data/ /home/$NB_USER/data
COPY engines/ /home/$NB_USER/engines
COPY ltr/ /home/$NB_USER/ltr
COPY semantic_search/ /home/$NB_USER/semantic_search
COPY webserver/ /home/$NB_USER/webserver
COPY build/ipython_kernel_config.py /etc/ipython/
RUN rm -rf work/

# Change to notebook user
RUN chown -R $NB_UID:$NB_UID /home/$NB_USER
RUN fix-permissions /home/$NB_USER
USER $NB_USER

# Spark Config
ENV SPARK_OPTS="$SPARK_OPTS --driver-java-options=\"-DXlint:none -Dlog4j.logLevel=error -Dallow-access=java.nio.DirectByteBuffer -Dlog4j.logger.org.apache.spark.repl.Main=ERROR\" --spark.ui.showConsoleProgress=False" \
    PYSPARK_SUBMIT_ARGS="-c spark.driver.defaultJavaOptions=\"-DXlint=none -Dlog4j.logLevel=error -Dallow-access=java.nio.DirectByteBuffer\" -c spark.ui.showConsoleProgress=False --jars $SHADED_SOLR_JAR_PATH,$SPARK_OS_PATH pyspark-shell" \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*-src.zip:%PYTHONPATH% \
    DOCKER_STACKS_JUPYTER_CMD=lab

# If you want to edit the notebooks and have your changes persist, 
# uncomment the line below and restart with `docker compose up --build`
#WORKDIR /tmp/notebooks

# Mark all notebooks as trusted by default
RUN find . -name \*.ipynb -print0|xargs -0 jupyter-trust -y

# Start Jupyter Notebooks
RUN jupyter labextension disable "@jupyterlab/apputils-extension:announcements"
CMD start-notebook.sh --ServerApp.password='' \
  --ServerApp.token='' --NotebookApp.token='' --LabApp.token='' \
  --LabApp.default_url='/lab/tree/chapters/welcome.ipynb' \
  --NotebookApp.allow_origin='*' --NotebookApp.ip='0.0.0.0' --ServerApp.ip=0.0.0.0 --no-browser
