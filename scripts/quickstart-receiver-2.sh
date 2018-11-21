#!/usr/bin/env bash
set -x

./thanos-remote-receive receiver \
    --tsdb.path                 data-receiver-2 \
    --debug.name                receiver-2 \
    --grpc-address              0.0.0.0:19092 \
    --http-address              0.0.0.0:19192 \
    --cluster.address           0.0.0.0:19392 \
    --cluster.advertise-address 127.0.0.1:19392 \
    --cluster.peers             127.0.0.1:19391 \
    --remote-write.address      0.0.0.0:19292 \
    --log.level=debug
