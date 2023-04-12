#!/usr/bin/env bash
source .env
redis-cli -h $REDISHOST -p $REDISPORT FLUSHALL
