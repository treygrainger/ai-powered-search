USERID=1000
SOLR_VERSION="9.4.1"
SOLR_DOWNLOAD_URL="https://archive.apache.org/dist/solr/solr/$SOLR_VERSION/solr-$SOLR_VERSION.tgz"
set -ex
wget -t 10 --max-redirect 1 --retry-connrefused -nv "$SOLR_DOWNLOAD_URL" -O "/opt/solr-$SOLR_VERSION.tgz"
tar -C /opt --extract --file "/opt/solr-$SOLR_VERSION.tgz"
(cd /opt; ln -s "solr-$SOLR_VERSION" solr)
rm "/opt/solr-$SOLR_VERSION.tgz"*
rm -Rf /opt/solr/docs/ /opt/solr/dist/{solr-core-$SOLR_VERSION.jar,solr-solrj-$SOLR_VERSION.jar,solrj-lib,solr-test-framework-$SOLR_VERSION.jar,test-framework}
mkdir -p /opt/solr/server/solr/lib /docker-entrypoint-initdb.d /opt/docker-solr
chown -R 0:0 "/opt/solr-$SOLR_VERSION"
find "/opt/solr-$SOLR_VERSION" -type d -print0 | xargs -0 chmod 0755
find "/opt/solr-$SOLR_VERSION" -type f -print0 | xargs -0 chmod 0644
chmod -R 0755 "/opt/solr-$SOLR_VERSION/bin" /opt/solr-$SOLR_VERSION/server/scripts/cloud-scripts
cp /opt/solr/bin/solr.in.sh /etc/default/solr.in.sh
mv /opt/solr/bin/solr.in.sh /opt/solr/bin/solr.in.sh.orig
mv /opt/solr/bin/solr.in.cmd /opt/solr/bin/solr.in.cmd.orig
chown root:0 /etc/default/solr.in.sh
chmod 0664 /etc/default/solr.in.sh
mkdir -p /var/solr/data /var/solr/logs /opt/solr/server/logs 
(cd /opt/solr/server/solr; cp solr.xml zoo.cfg /var/solr/data/)
cp /opt/solr/server/resources/log4j2.xml /var/solr/log4j2.xml
find /var/solr -type d -print0 | xargs -0 chmod 0770
find /var/solr -type f -print0 | xargs -0 chmod 0660
chown -R $USERID:$USERID /opt/solr-$SOLR_VERSION /docker-entrypoint-initdb.d /opt/docker-solr
chown -R $USERID:$USERID /var/solr
chown -R $USERID:$USERID /opt/solr
chmod 0770 /opt/solr/*

set -ex
(cd /opt; ln -s solr-*/ solr)
rm -Rf /opt/solr/docs /opt/solr/docker/Dockerfile

apt-get update
apt-get -y --no-install-recommends install acl lsof procps wget netcat gosu tini jattach openjdk-17-jre
rm -rf /var/lib/apt/lists/*

DISTRO_NAME=zookeeper-3.4.5
wget -q "http://archive.apache.org/dist/zookeeper/$DISTRO_NAME/$DISTRO_NAME.tar.gz"
tar -xzf "$DISTRO_NAME.tar.gz"
rm -rf $DISTRO_NAME.tar.gz
mkdir -p /data/zk
chmod 0770 $DISTRO_NAME/*
printf "tickTime=2000\ninitLimit=10\nsyncLimit=5\ndataDir=/data/zk\nclientPort=2181" >> $DISTRO_NAME/conf/zoo.cfg
chown $USERID:$USERID $DISTRO_NAME /data/zk