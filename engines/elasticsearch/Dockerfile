FROM elasticsearch:8.16.0

# Copy custom configuration files if needed
COPY elasticsearch.yml /usr/share/elasticsearch/config/

# Set environment variables
ENV discovery.type=single-node
ENV xpack.security.enabled=false
ENV ES_JAVA_OPTS="-Xms512m -Xmx512m"

# Expose ports
EXPOSE 9200 9300

# Set healthcheck
HEALTHCHECK --interval=5s --timeout=5s --retries=10 \
  CMD curl -s http://localhost:9200/_cluster/health | grep -q '"status":"green"'

# Run as elasticsearch user
USER elasticsearch
