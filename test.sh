#!/bin/bash

#Exit on error
set -e

#Get throughput and latency
killall -9 kv_service || true;

#Set num replicas
num_replicas=16

#Generate keys / certificates for each node 
echo "generating keys and certificates for 16 replicas and 1 client"
./node-auth.sh $num_replicas
sleep 10

# Initialize the servers
echo "starting service..."
./service/tools/kv/server_tools/start_kv_service.sh &
echo "Waiting for build to finish"
sleep 20
echo "Build finished"
echo ""

SERVICE_PID=$!
echo "service started in background (PID: $SERVICE_PID)."

#Build bazel api tools
echo "Building bazel api-tools..."
bazel build service/tools/kv/api_tools/kv_service_tools
echo "Built bazel api-tools successfully."

#Run KV-set benchmark
SECONDS=0
SRV_TOOL="bazel-bin/service/tools/kv/api_tools/kv_service_tools"
CONF="service/tools/config/interface/service.config"

echo "Beginning basic benchmark"

while [ $SECONDS -le 30 ]
do
    for j in $(seq 1 1)
    do
        echo "setting key $j"
        # $SRV_TOOL --config $CONF --cmd set --key $j --value $SECONDS
        $SRV_TOOL $CONF set $j $SECONDS > /dev/null
        echo "Setting key = $j value = $SECONDS"
    done
    sleep 1
done

sleep 120

#Stop the servers
killall -9 kv_service
echo "Benchmark complete."

#Get throughput and latency
log_files=""
for log in $(seq 0 $((num_replicas - 1)))
do
    log_name="server$log.log"
    echo "Found log: $log_name"
    echo ""
    log_files="$log_files $log_name"
done
echo "Found log: client.log"
echo ""
client_log="client.log"

# Pass all server logs and client log to the Python script
echo "Calculating throughput and latency"
python3 scripts/deploy/performance/calculate_result.py $log_files $client_log
echo "Done."

