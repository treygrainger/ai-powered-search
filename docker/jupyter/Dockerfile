FROM python:3.7-stretch

# Install openjdk
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk && \
    apt-get clean;

# Setup a user 
RUN useradd -ms /bin/bash aips 
WORKDIR /home/aips

# Pull Requirements, Install Notebooks
COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY notebooks /home/aips/notebooks/
WORKDIR /home/aips/notebooks

USER aips

CMD jupyter notebook --ip=0.0.0.0 --no-browser --NotebookApp.token='' 