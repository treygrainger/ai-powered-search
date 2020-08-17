#!/bin/sh 
mkdir -p /var/solr/data/lib/
cp dist/solr-ltr-*.jar /var/solr/data/lib/
ls /var/solr/data/lib

solr-foreground -Dsolr.ltr.enabled=true
