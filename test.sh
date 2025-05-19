killall -9 kv_service

echo "starting service..."

./service/tools/kv/server_tools/start_kv_service.sh

echo "service started."

SECONDS=0
SRV_TOOL="bazel-bin/service/tools/kv/api_tools/kv_service_tools"
CONF="service/tools/config/interface/service.config"

echo "beginning basic benchmark"

while [ $SECONDS -le 30 ]
do
    for j in $(seq 1 1000)
    do
    $SRV_TOOL --config $CONF --cmd set --key $j --value $SECONDS > /dev/null
    done
    echo $SECONDS
done

# killall -9 kv_service

echo "done"