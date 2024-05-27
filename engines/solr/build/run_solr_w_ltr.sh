#!/bin/sh 
mkdir -p /var/solr/data/lib/
cp dist/solr-ltr-*.jar /var/solr/data/lib/
ls /var/solr/data/lib

SOLR_MODULES=ltr
solr-foreground -Dsolr.modules=ltr -Dsolr.ltr.enabled=true 