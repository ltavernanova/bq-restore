# Dockerfile
FROM google/cloud-sdk:slim

WORKDIR /app

# Instala jq para parsear JSON
RUN apt-get update \
 && apt-get install -y --no-install-recommends jq \
 && rm -rf /var/lib/apt/lists/*

COPY restore.sh .

RUN chmod +x restore.sh

ENTRYPOINT ["bash", "restore.sh"]
