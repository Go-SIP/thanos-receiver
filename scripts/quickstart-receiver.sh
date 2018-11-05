#!/usr/bin/env bash
set -x

./thanos-remote-receive receiver \
    --debug.name                receiver-1 \
    --grpc-address              0.0.0.0:19091 \
    --http-address              0.0.0.0:19191 \
    --cluster.address           0.0.0.0:19391 \
    --cluster.advertise-address 127.0.0.1:19391 \
    --cluster.peers             127.0.0.1:19391 \
    --log.level=debug
