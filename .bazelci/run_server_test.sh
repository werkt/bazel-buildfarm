#!/bin/bash

if [ -z "$BAZEL" ]; then
  BAZEL=bazel
fi

# Start redis container
docker run -d --rm --name buildfarm-redis --network host redis:5.0.9 --bind localhost

# Build worker and server targets
$BAZEL build //src/main/java/build/buildfarm:buildfarm-shard-worker
$BAZEL build //src/main/java/build/buildfarm:buildfarm-server

# Start a single worker
$BAZEL run //src/main/java/build/buildfarm:buildfarm-shard-worker $(pwd)/examples/config.minimal.yml > worker.log 2>&1 &
worker=$!
echo "Started buildfarm-shard-worker..."

# Start a single server
$BAZEL run //src/main/java/build/buildfarm:buildfarm-server $(pwd)/examples/config.minimal.yml > server.log 2>&1 &
server=$!
echo "Started buildfarm-server..."

echo "Wait for startup to finish..."
sleep 30
echo "Printing server initialization logs..."
cat server.log
echo "Printing worker initialization logs..."
cat worker.log

# Build bazel targets with buildfarm
echo "Running server integration tests..."
$BAZEL test --test_tag_filters=integration src/test/java/build/buildfarm/server:all
status=$?

# initiate shutdown of server/worker
kill -INT $server
kill -INT $worker

# ensure worker/server termination
wait

# clear out our running redis container
docker kill buildfarm-redis

# exit with test status
exit $status
