# Welcome to kvservice


**Kvserivce** is service with key/value supports key/value interface via Restful api, 
where key is string and value is string or primitive data type, or list of these data type. It was developed at Coccoc 
Data engineering team based on [Facebook RocksDb](https://github.com/facebook/rocksdb/wiki)

**Kvservice** use RocksDb as storage engine and use [Java JNI Rocksdb](https://github.com/facebook/rocksdb/wiki/RocksJava-Basics) to 
manipulate with RocksDb, and use [Google protobuf](https://developers.google.com/protocol-buffers) for 
serialize and deserialize data. It uses [Grpc](https://grpc.io) for intercommunicate between server each other, and [Java Netty](https://netty.io)
to build transport service, client use RestFul api to communicate with transport service and Kvservice

Because Kvservice use RocksDb, it is very fast, use state of the art technology, and compatibility with multiple storage 
technology as Flash, Hard disk. It simply in the design and in the implementation, because of that, it doesn't be penalized 
by extra synchronize mechanism as [Cassandra](https://cassandra.apache.org), and it doesn't support complex data type, 
complex ACID operator, it is simply and very fast

-------

## Alternatives
* [SSDB](http://ssdb.io): This is good alternative for Redis, but it doesn't meet our requirements related throughput
* [Tendis](http://tendis.cn/#/): This is an alternative come from Tencent, but it is optimized for SSD, and hard to maintain
---------

## Features
* Support data with volume upto Terabyte and the hundred million of keys
* Support multiple storage technology as HHD, SSD or on the cloud
* Consume little of memory
* Very high throughput 
* Ease use of API

--------

## Get started 
For the user want start with [Kvservice API](https://git.itim.vn/ads-targeting/listdb/wikis/Kvservice-API)
For the user want dive deep into how Kvservice work [Kv service architecture]()

----------
## Contributing to Kvservice
You are welcome to send pull requests to contribute to Kvservice code base

----------
## Contact
<phongnh@coccoc.com>
