#!/usr/bin/env bash

flower --address=0.0.0.0 --port=5555 --broker="amqp://couchbase:couchbase@ci.sc.couchbase.com:5672/broker"
