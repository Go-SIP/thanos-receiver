#!/usr/bin/env bash
set -x

/tmp/thanos-0.1.0.linux-amd64/thanos query \
    --debug.name                query-1 \
    --grpc-address              0.0.0.0:19991 \
    --http-address              0.0.0.0:19491 \
    --cluster.address           0.0.0.0:19591 \
    --cluster.advertise-address 127.0.0.1:19591 \
    --cluster.peers             127.0.0.1:19391 \
    --log.level=debug
