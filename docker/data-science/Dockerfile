FROM jupyter/pyspark-notebook:spark-3.1.2

USER root

#install gcc, c++, and related dependencies needed to for pip to build some python dependencies
RUN sudo apt-get update && apt-get install -y --reinstall  build-essential

# Spark dependencies
ENV SPARK_SOLR_VERSION=4.0.2
ENV SHADED_SOLR_JAR_PATH=/usr/local/spark/lib/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar

# Install Spark-Solr
RUN mkdir -p /usr/local/spark/lib/ && cd /usr/local/spark/lib/ && \
    wget -q https://repo1.maven.org/maven2/com/lucidworks/spark/spark-solr/${SPARK_SOLR_VERSION}/spark-solr-${SPARK_SOLR_VERSION}-shaded.jar -O $SHADED_SOLR_JAR_PATH && \
    echo "c5293f10257603bcf650780afcb91ed1bb118f09feb731502c2dc7ac14ba950e586a033cb2f50e5c122c5ec442dc0d2b55f76c4f6522b555e67f4981a38bca26 *spark-solr-${SPARK_SOLR_VERSION}-shaded.jar" | sha512sum -c - && \
    chmod a+rwx $SHADED_SOLR_JAR_PATH

# Pull Requirements, Install Notebooks
COPY requirements.txt ./
RUN python -m pip --no-cache-dir install --upgrade pip && \
  pip --no-cache-dir install -r requirements.txt

#must be installed after other requirements for now due to spacy/neuralcoref compatability issues
RUN apt-get -y update && \
    apt-get install --no-install-recommends -y build-essential && \
    rm -rf /var/lib/apt/lists/* && \
    pip --no-cache-dir install git+https://github.com/huggingface/neuralcoref.git --no-binary neuralcoref && \
    apt-get purge -y build-essential && apt-get auto-remove -y && apt-get auto-clean -y

RUN python -m spacy download en_core_web_sm
#RUN python -m spacy download en_core_web_lg

COPY notebooks notebooks
#RUN conda install python=3.7.12

# Install pyarrow
#RUN conda install --quiet -y 'pyarrow' && \
#    conda clean --all -f -y && \
#    fix-permissions $CONDA_DIR && \
#    fix-permissions /home/$NB_USER

# Install tensorflow
#RUN pip install 'tensorflow==2.1.0' && \
#    fix-permissions $CONDA_DIR && \
#    fix-permissions /home/$NB_USER

# Setup an AI-Powered Search user
#RUN useradd -ms /bin/bash aips

WORKDIR /home/$NB_USER

# Pull Requirements, Install Notebooks
COPY requirements.txt ./
ENV BLIS_ARCH="generic" 
RUN python -m pip --no-cache-dir install --upgrade pip && \
  pip --no-cache-dir install -r requirements.txt

RUN python -m spacy download en_core_web_sm
#RUN python -m spacy download en_core_web_lg

#must be installed after other requirements for now due to spacy/neuralcoref compatability issues
RUN pip --no-cache-dir install git+https://github.com/huggingface/neuralcoref.git@654d90659e34f78b98681ccde7bdda4558aa21c2 --no-binary neuralcoref

#RUN git clone https://github.com/ai-powered-search/retrotech.git $WORKDIR/retrotech

#COPY notebooks notebooks

#RUN chown -R $NB_UID:$NB_UID /home/$NB_USER
#RUN fix-permissions /home/$NB_USER
#RUN fix-permissions notebooks

RUN chown -R $NB_UID:$NB_UID /home/$NB_USER
USER $NB_UID

# Spark Config
ENV SPARK_OPTS="$SPARK_OPTS --spark.driver.extraLibraryPath=$SHADED_SOLR_JAR_PATH --spark.executor.extraLibraryPath=$SHADED_SOLR_JAR_PATH" \
    PYSPARK_SUBMIT_ARGS="--jars $SHADED_SOLR_JAR_PATH pyspark-shell" \
    PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip

WORKDIR notebooks
# If you want to edit the notebooks and have
# your changes persist, uncomment the line below
# and restart with `docker-compose up --build`

#WORKDIR /tmp/notebooks

#todo: require password for security. Turning off in first release to avoid initial complexity for readers.
CMD jupyter notebook --ip=0.0.0.0 --no-browser --NotebookApp.token='' --NotebookApp.password=''
