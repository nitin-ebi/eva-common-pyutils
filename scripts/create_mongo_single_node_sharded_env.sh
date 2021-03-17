# Adapted from https://stackoverflow.com/a/56264776
function wait_for_mongo() {
  # Wait until Mongo is ready to accept connections, exit if this does not happen within 30 seconds
  COUNTER=0
  until mongo --host $1 --eval "printjson(db.serverStatus())"
  do
    sleep 1
    COUNTER=$((COUNTER+1))
    if [[ ${COUNTER} -eq 120 ]]; then
      echo "MongoDB did not initialize within 2 minutes, exiting"
      exit 2
    fi
    echo "Waiting for MongoDB to initialize... ${COUNTER}/120"
  done
}

export mongodb_version=$1
wget http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-${mongodb_version}.tgz
tar xfz mongodb-linux-x86_64-${mongodb_version}.tgz
export PATH=`pwd`/mongodb-linux-x86_64-${mongodb_version}/bin:$PATH
mongod --version
rm -rf /data/mongodb
mkdir -p /data/mongodb/shard01 /data/mongodb/shard02 /data/mongodb/config
mongod --shardsvr --port 27018 --replSet rs0 --dbpath /data/mongodb/shard01 &
wait_for_mongo "localhost:27018"
mongod --shardsvr --port 27020 --replSet rs0 --dbpath /data/mongodb/shard02 &
wait_for_mongo "localhost:27020"
mongo --port 27018 --eval 'rs.initiate({_id:"rs0", members: [{_id: 1, host: "localhost:27018"}, {_id: 2, host: "localhost:27020"}]})'

mongod --configsvr --port 27019 --replSet rs1 --dbpath /data/mongodb/config &
wait_for_mongo "localhost:27019"
mongo --port 27019 --eval 'rs.initiate()'
mongos --configdb rs1/localhost:27019 --port 27017 &
wait_for_mongo "localhost:27017"
mongo --eval 'sh.addShard("rs0/localhost:27018,localhost:27020")'
