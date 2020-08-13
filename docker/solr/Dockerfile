FROM solr:8.5.2

USER root

# Copy in LTR jar from dist for each node...
CMD mkdir -p /var/solr/data/lib/
CMD cp dist/solr-ltr-*.jar /var/solr/lib/
CMD chown solr:solr /var/solr/lib

USER solr

CMD ["solr-foreground", "-Dsolr.ltr.enabled=true"]
