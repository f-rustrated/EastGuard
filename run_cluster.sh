#!/bin/bash
set -e

CLUSTER_DIR=".test_cluster"

if [ "$1" == "clean" ]; then
    echo "Cleaning up cluster data..."
    rm -rf $CLUSTER_DIR node1.log node2.log node3.log
    exit 0
fi

if [ "$1" == "stop" ]; then
    echo "Stopping cluster..."
    pkill -f "target/debug/server --advertise-host 127.0.0.1" || echo "No cluster running."
    exit 0
fi

echo "Building server..."
cargo build --bin server
echo "Building cli..."
cargo build --bin eg-cli

# Start fresh by default
rm -rf $CLUSTER_DIR node1.log node2.log node3.log
mkdir -p $CLUSTER_DIR

echo "Starting Node 1..."
CLIENT_PORT=2921 CLUSTER_PORT=2922 DATA_PORT=2923 DATA_DIR=$CLUSTER_DIR/data1 META_DIR=$CLUSTER_DIR/meta1 target/debug/server --advertise-host 127.0.0.1 > node1.log 2>&1 &
NODE1_PID=$!

echo "Starting Node 2..."
CLIENT_PORT=2931 CLUSTER_PORT=2932 DATA_PORT=2933 DATA_DIR=$CLUSTER_DIR/data2 META_DIR=$CLUSTER_DIR/meta2 target/debug/server --advertise-host 127.0.0.1 --join-seed-nodes 127.0.0.1:2922 > node2.log 2>&1 &
NODE2_PID=$!

echo "Starting Node 3..."
CLIENT_PORT=2941 CLUSTER_PORT=2942 DATA_PORT=2943 DATA_DIR=$CLUSTER_DIR/data3 META_DIR=$CLUSTER_DIR/meta3 target/debug/server --advertise-host 127.0.0.1 --join-seed-nodes 127.0.0.1:2922 > node3.log 2>&1 &
NODE3_PID=$!

echo "Waiting for cluster to stabilize (10s)..."
sleep 10

echo "Cluster is up! You can now use the CLI: cargo run --bin eg-cli"
echo "To stop the cluster: bash run_cluster.sh stop"
echo "To clean up data: bash run_cluster.sh clean"
