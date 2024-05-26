#!/bin/sh

docker build . -t aips-solr
docker-compose up
