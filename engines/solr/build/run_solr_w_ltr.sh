#!/bin/sh 
mkdir -p /var/solr/data/

SOLR_MODULES=ltr
solr-foreground -Dsolr.modules=ltr -Dsolr.ltr.enabled=true -Dlog4j2.configurationFile=/opt/solr-9.4.1/log4j2-config.xml -m 1g