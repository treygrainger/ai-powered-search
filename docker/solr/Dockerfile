FROM solr:9.4.1

USER root

ADD run_solr_w_ltr.sh ./run_solr_w_ltr.sh
RUN chown solr:solr run_solr_w_ltr.sh
RUN sed -i -e 's/\r$//' run_solr_w_ltr.sh
RUN chmod u+x run_solr_w_ltr.sh

USER solr

ENTRYPOINT "./run_solr_w_ltr.sh" 
