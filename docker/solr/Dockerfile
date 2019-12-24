FROM solr:8.3.1

USER root

ADD ch3-apple-juice server/solr/configsets/ch3-apple-juice

RUN chown solr:solr server/solr/configsets/ch3-apple-juice

USER solr

CMD ["solr-foreground", "-Dsolr.ltr.enabled=true"]
