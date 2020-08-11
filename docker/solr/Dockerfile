FROM solr:8.3.1

USER root

# Tweak _default configset to add LTR
RUN sed '$ d' server/solr/configsets/_default/conf/solrconfig.xml > /tmp/solrconfig_snipped.xml

RUN cat /tmp/solrconfig_snipped.xml
ENV SOLR_LTR_CONFIG="<queryParser name=\"ltr\" class=\"org.apache.solr.ltr.search.LTRQParserPlugin\"/> \n\n <transformer name=\"features\" class=\"org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory\"> <str name=\"fvCacheName\">QUERY_DOC_FV</str> </transformer> \n\n </config>  "

RUN echo $SOLR_LTR_CONFIG >> /tmp/solrconfig_snipped.xml
RUN cat /tmp/solrconfig_snipped.xml

RUN chown solr:solr server/solr/configsets/_default/
RUN cp /tmp/solrconfig_snipped.xml server/solr/configsets/_default/conf/solrconfig.xml

USER solr

RUN cat server/solr/configsets/_default/conf/solrconfig.xml
CMD ["solr-foreground", "-Dsolr.ltr.enabled=true"]
