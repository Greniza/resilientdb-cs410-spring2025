# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

#Stop all pre-existing services
killall -9 kv_service 2>/dev/null || true

SERVER_PATH=./bazel-bin/service/kv/kv_service
SERVER_CONFIG=service/tools/config/server/server.config
WORK_PATH=$PWD
CERT_PATH=${WORK_PATH}/service/tools/data/cert

#Build the key-value service
bazel build //service/kv:kv_service $@ && echo "Building KV Service completed successfully"

#Starting the key-value service
num_replicas=16
num_clients=1
pids=()
replica_pids=()
client_pids=()
startup_timeout=60

# First clean up any existing logs
echo "Checking and removing old log files."
# Remove replica logs (0 to num_replicas-1)
for node_num in $(seq 0 $((num_replicas-1))); do
    log_name="server$node_num.log"
    if [ -f "$log_name" ]; then
        echo "Removing $log_name"
        rm -f "$log_name"
    fi
done

# Remove client logs
if [ -f "client.log" ]; then
    echo "Removing client.log"
    rm -f "client.log"
fi

echo "Log cleanup completed"
echo ""

#Rebuild configuration files
echo "Rebuilding certificates and keys for $num_replicas replicas and $num_clients clients."
echo ""
./node-auth.sh $num_replicas $num_clients

# Function to check if a server is ready (0: Ready, 1: Not Ready)
check_server_ready() {
    local node_num=$1
    local log_file=$2
    local ready_message="Server $node_num is ready"
    #Check if all replicas can connect before claiming ready
    # local ready_message="receive public size:16"
    if [ -f "$log_file" ] && grep -q "$ready_message" "$log_file"; then
        return 0
    else
        return 1
    fi
}

wait_for_all_servers_ready() {
    local max_wait=$1
    local start_time=$(date +%s)
    local ready_servers=()
    local total_replicas=$num_replicas
    
    echo "Monitoring all $total_replicas replica servers for readiness..."
    echo "Max wait time: ${max_wait} seconds"
    echo ""
    
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        # Check if we've exceeded timeout
        if [ $elapsed -ge $max_wait ]; then
            echo "TIMEOUT: Not all servers became ready within ${max_wait} seconds"
            echo "Ready servers: ${#ready_servers[@]}/$total_replicas"
            
            # Show which servers are not ready
            for node_num in $(seq 0 $((num_replicas-1))); do
                if [[ ! " ${ready_servers[@]} " =~ " ${node_num} " ]]; then
                    echo "  Server $node_num: NOT READY"
                    echo "    Last 5 lines of server${node_num}.log:"
                    tail -5 "server${node_num}.log" 2>/dev/null | sed 's/^/      /' || echo "      (log file not readable)"
                fi
            done
            return 1
        fi
        
        # Check each replica server
        local newly_ready=()
        for node_num in $(seq 0 $((num_replicas-1))); do
            # Skip if already marked as ready
            if [[ " ${ready_servers[@]} " =~ " ${node_num} " ]]; then
                continue
            fi
            
            # Check if this server is now ready
            if check_server_ready $node_num "server${node_num}.log"; then
                ready_servers+=($node_num)
                newly_ready+=($node_num)
                echo "Server $node_num is ready! (${#ready_servers[@]}/$total_replicas ready)"
            fi
        done
        
        # Check if all servers are ready
        if [ ${#ready_servers[@]} -eq $total_replicas ]; then
            echo ""
            echo "All $total_replicas replica servers are ready! (took ${elapsed} seconds)"
            return 0
        fi
        
        # Progress indicator every 10 seconds
        if [ $((elapsed % 10)) -eq 0 ] && [ $elapsed -gt 0 ]; then
            echo "  Progress: ${#ready_servers[@]}/$total_replicas servers ready (${elapsed}s elapsed)"
            
            # Show status of servers that aren't ready yet
            local not_ready_count=0
            for node_num in $(seq 0 $((num_replicas-1))); do
                if [[ ! " ${ready_servers[@]} " =~ " ${node_num} " ]]; then
                    if [ $not_ready_count -lt 3 ]; then 
                        echo "    Server $node_num: $(tail -1 "server${node_num}.log" 2>/dev/null | cut -c1-80)..." || echo "    Server $node_num: (no log yet)"
                    fi
                    ((not_ready_count++))
                fi
            done
            if [ $not_ready_count -gt 3 ]; then
                echo "    ... and $((not_ready_count - 3)) more servers still starting"
            fi
        fi
        
        sleep 1
    done
}

# Function to wait for client ready message
wait_for_client_ready() {
    local max_wait=$1
    local client_num=$2
    local log_file="client.log"  # Use original log file name
    local ready_message="Server $client_num is ready"  
    # local ready_message="receive public size:17"
    
    echo "Waiting for client (node $client_num) to be ready..."
    echo "Monitoring log file: $log_file"
    echo "Max wait time: ${max_wait} seconds"
    
    local start_time=$(date +%s)
    
    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))
        
        # Check if we've exceeded timeout
        if [ $elapsed -ge $max_wait ]; then
            echo "TIMEOUT: Client did not become ready within ${max_wait} seconds"
            echo "Last 10 lines of $log_file:"
            tail -10 "$log_file" 2>/dev/null || echo "  (log file not readable)"
            return 1
        fi
        
        # Check if log file exists and contains ready message
        if [ -f "$log_file" ] && grep -q "$ready_message" "$log_file"; then
            echo "✓ Client is ready! (took ${elapsed} seconds)"
            return 0
        fi
        
        # Progress indicator
        if [ $((elapsed % 5)) -eq 0 ] && [ $elapsed -gt 0 ]; then
            echo "  Still waiting for client... (${elapsed}s elapsed)"
            
            # Show last line of log file for debugging
            if [ -f "$log_file" ]; then
                echo "  Last log entry: $(tail -1 "$log_file" 2>/dev/null | cut -c1-100)..."
            fi
        fi
        
        sleep 1
    done
}

# Function to check if process is still running
check_process_running() {
    local pid=$1
    local node_num=$2
    
    if ! kill -0 $pid 2>/dev/null; then
        echo "ERROR: Server $node_num process (PID: $pid) has died!"
        return 1
    fi
    return 0
}

# PHASE 1: Start all replica servers
echo "=== PHASE 1: Starting all replica servers ==="
echo "Starting $num_replicas replica servers (nodes 0-$((num_replicas-1)))..."
echo ""

for node_num in $(seq 0 $((num_replicas-1))); do
    echo "[$((node_num+1))/$num_replicas] Starting replica server: ${node_num}..."
    log_file="server${node_num}.log"

    # Start Replica
    nohup $SERVER_PATH $SERVER_CONFIG $CERT_PATH/node${node_num}.key.pri $CERT_PATH/cert_${node_num}.cert > server${node_num}.log 2>&1 &
    pid=$!
    pids+=($pid)
    replica_pids+=($pid)
    echo "  Started with PID: $pid"

    # Brief pause between starts
    sleep 0.01

    # Quick check if process started (but don't wait for ready)
    if ! check_process_running $pid $node_num; then
        echo "FATAL: Server $node_num failed to start properly"
        echo "Check the log file: $log_file"
        exit 1
    fi
done

echo ""
echo "All $num_replicas replica servers have been started"
echo "Replica PIDs: ${replica_pids[*]}"
echo ""

# PHASE 2: Wait for all replicas to be ready
echo "=== PHASE 2: Waiting for all replicas to be ready ==="
if ! wait_for_all_servers_ready $startup_timeout; then
    echo "FATAL: Not all replica servers became ready"
    echo "This will prevent proper cluster operation"
    
    # Show which processes are still running
    echo ""
    echo "Process status check:"
    for i in "${!replica_pids[@]}"; do
        pid=${replica_pids[$i]}
        node_num=$i
        if check_process_running $pid $node_num; then
            echo "  Server $node_num (PID $pid): RUNNING but not ready"
        else
            echo "  Server $node_num (PID $pid): DEAD"
        fi
    done
    
    exit 1
fi

# PHASE 3: Start clients
echo "=== PHASE 3: Starting clients ==="
echo "Starting $num_clients client(s) (node $num_replicas)..."

# For simplicity, start client as node 16 (matching original script)
client_num=$num_replicas
echo "Starting client (node $client_num)..."
log_file="client.log"

nohup $SERVER_PATH $SERVER_CONFIG $CERT_PATH/node${client_num}.key.pri $CERT_PATH/cert_${client_num}.cert > client.log 2>&1 &
pid=$!
pids+=($pid)
client_pids+=($pid)

echo "Started client with PID: $pid"

# Brief pause
sleep 2

# Check if client process started
if ! check_process_running $pid $client_num; then
    echo "FATAL: Client process failed to start properly"
    echo "Check the log file: $log_file"
    exit 1
fi

# Wait for client to be ready (looking for "Server 16 is ready")
if ! wait_for_client_ready $startup_timeout $client_num; then
    echo "WARNING: Client did not become ready within timeout"
    echo "Client may still be initializing - check client.log"
else
    echo "Client started successfully"
fi

echo ""
echo "=== STARTUP COMPLETE ==="
echo "Replicas: ${#replica_pids[@]} started (PIDs: ${replica_pids[*]})"
echo "Clients: ${#client_pids[@]} started (PIDs: ${client_pids[*]})"
echo "Total processes: ${#pids[@]}"
