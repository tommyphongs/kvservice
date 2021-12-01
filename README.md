#Kvservice: Fast, use low memory kv service

## How to build:
* Install protobuf compile
* Compile proto file:  `protoc -I=./proto --java_out=./src/main/java ./proto/*`
* Build: `MAIN_CLASS=com.coccoc.bi.kvservice.Main mvn clean compile assembly:single`
* Run: `java -jar target/com.coccoc.bi.kvservice-{VERSION}-jar-with-dependencies`  

## Build docker file 

## How to run
* With compile maven plugin: ``` mvn compile && DATA_DIR=${DATA_DIR} HTTP_PORT=${HTTP_PORT}  mv exec:java -Dexec.mainClass="com.coccoc.bi.kvservice"```
* With env file: ``` . ad2v.env && mvn compile && HTTP_PORT=${HTTP_PORT} DATA_DIR=${DATA_DIR} GRPC_PORT=${GRPC_PORT} MODE=${MODE} LOG4J_CONF_FILE=${LOG4J_CONF_FILE} LOG_DIR=${LOG_DIR} mvn exec:java -Dexec.mainClass="com.coccoc.bi.kvservice.Main" -DLOG_DIR=$LOG_DIR```

## Benchmark
* Load benchmark: ``` mvn compile exec:java -Dexec.mainClass="com.coccoc.bi.kvservice.benchmark.Benchmark" -Dexec.args="--type load --numberOfKey 4000 --arrLoadSize 500 --nThread 8 --serverUrl http://ads-target2v.dev.itim.vn:9898"
* Read benchmark: ``` mvn compile exec:java -Dexec.mainClass="com.coccoc.bi.kvservice.benchmark.Benchmark" -Dexec.args="--type read --numberOfKey 40000000 --batchSize 1000 --numberOfOps 1000000 --arrReadSize 500 --nThread 8 --serverUrl http://ads-target3v.dev.itim.vn:8888" ```


## Environment variable: 
* DATA_DIR: Dir to persist data
* HTTP_PORT: Port to http service bind to
* GRPC_PORT: Port to grpc service bind to