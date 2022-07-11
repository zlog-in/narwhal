readonly CONTAINER_NAME="narwhal"
node_index=$(tail -n 1 index.txt)
echo $node_index
rm -rf narwhal/logs/*.log
docker cp index.txt $CONTAINER_NAME:/home/narwhal/benchmark/
docker exec -it $CONTAINER_NAME bash
