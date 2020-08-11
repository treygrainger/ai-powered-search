FROM solr:8.5.1

USER root

# Tweak _default configset to add LTR
RUN sed '$ d' server/solr/configsets/_default/conf/solrconfig.xml > /tmp/solrconfig_snipped.xml

RUN cat /tmp/solrconfig_snipped.xml
ENV LTR_LIB_CFG="<lib dir=\"\${solr.install.dir:../../../..}/dist/\" regex=\"solr-ltr-\d.*\.jar\" />\n\n"
ENV LTR_QUERY_PARSER_CFG="<queryParser name=\"ltr\" class=\"org.apache.solr.ltr.search.LTRQParserPlugin\"/> \n\n"
ENV LTR_TRANSFORMER_CFG="<transformer name=\"features\" class=\"org.apache.solr.ltr.response.transform.LTRFeatureLoggerTransformerFactory\"> <str name=\"fvCacheName\">QUERY_DOC_FV</str> </transformer> \n\n </config>  "
ENV SOLR_LTR_CONFIG="$LTR_LIB_CFG $LTR_QUERY_PARSER_CFG $LTR_TRANSFORMER_CFG" 

RUN echo $SOLR_LTR_CONFIG >> /tmp/solrconfig_snipped.xml
RUN cat /tmp/solrconfig_snipped.xml

RUN chown solr:solr server/solr/configsets/_default/
RUN cp /tmp/solrconfig_snipped.xml server/solr/configsets/_default/conf/solrconfig.xml
RUN cat server/solr/configsets/_default/conf/solrconfig.xml

USER solr

RUN cat server/solr/configsets/_default/conf/solrconfig.xml
CMD ["solr-foreground", "-Dsolr.ltr.enabled=true"]
