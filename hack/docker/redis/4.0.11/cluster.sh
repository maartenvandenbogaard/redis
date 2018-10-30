#!/bin/bash

set -e

# needs args in following manner:
#    src_node_ip
#    src_node_id
#    dst_node_ip
#    dst_node_id
#    slot_start
#    slot_end
reshard() {
    for i in `seq $5 $6`; do
        redis-cli -c -h $3 cluster setslot ${i} importing $2
        redis-cli -c -h $1 cluster setslot ${i} migrating $4
        while true; do
            key=`redis-cli -c -h $1 cluster getkeysinslot ${i} 1`
            if [ "" = "$key" ]; then
                echo "there are no key in this slot ${i}"
                break
            fi
            redis-cli -h $1 migrate $3 6379 ${key} 0 5000
        done
        redis-cli -c -h $1 cluster setslot ${i} node $4
        redis-cli -c -h $3 cluster setslot ${i} node $4
    done
}

"$@"
