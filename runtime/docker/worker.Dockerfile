# Both arguments must be immutable registry digests selected by the release builder.
ARG NODE_IMAGE
ARG DOCKER_CLI_IMAGE
FROM ${DOCKER_CLI_IMAGE} AS docker-cli
FROM ${NODE_IMAGE} AS runtime
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git python3 ca-certificates coreutils \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/orch
COPY runtime/package.json runtime/package-lock.json ./
RUN npm ci --ignore-scripts
COPY runtime/tsconfig.json ./
COPY runtime/src ./src
RUN npm run build && npm prune --omit=dev --ignore-scripts && rm -rf src
COPY src/stage34/core/worker_rpc.py /opt/orch/worker_rpc.py
COPY --chmod=0444 src/stage34/core/file_tools.py /opt/orch/file_tools.py
COPY --chmod=0555 src/stage34/core/docker_broker.py /opt/orch/docker_broker.py
RUN mkdir -p /home/worker && chown 1000:1000 /home/worker
ENV HOME=/home/worker
USER 1000:1000
FROM runtime AS job
ENTRYPOINT ["/usr/local/bin/node", "/opt/orch/dist/job-child.js"]
FROM runtime AS worker
USER root
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/node", "/opt/orch/dist/worker-daemon.js"]
