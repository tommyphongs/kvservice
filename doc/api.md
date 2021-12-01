# ListDB api 

## Description
Kvservice use RESTFul API for user manipulate with Kvservice server

## Api related to list data structures
### Peek
Get the elements without remove them
#### Peek tail
Peek from the tail of the list

##### Use POST HTTP request
```
curl --location --request POST 'http://{{host}}:{{port}}/api/list/tailpeek' \
--header 'Content-Type: application/json' \
--data-raw '{
    "keys": [Key1, Key2, ...., Keyn],
    "size": size
}'

host: The host of kvservice instance running, including master and slave
port: The port of kvservice instance running
keys: List of the keys to peek
size: Number of elements to peek
```

##### Use Get HTTP request
We don't support batch peek with GET request
```
curl --location --request GET
 'http://{{host}}:{{port}}/api/list/tailpeek?key=Key&size=size'
 
host: The host of kvservice instance running, including master and slave
port: The port of kvservice instance running
Key: The key to peek
size: Number of elements to peek
```

#### Peek head
Peek from the head of the list

##### Use POST HTTP request
```
curl --location --request POST 'http://{{host}}:{{port}}/api/list/headpeek' \
--header 'Content-Type: application/json' \
--data-raw '{
    "keys": [Key1, Key2, ...., Keyn],
    "size": size
}'

host: The host of kvservice instance running, including master and slave
port: The port of kvservice instance running
keys: List of the keys to peek
size: Number of elements to peek
```

##### Use Get HTTP request
We don't support batch peek with GET request
```
curl --location --request GET
 'http://{{host}}:{{port}}/api/list/headpeek?key=Key&size=size'
 
host: The host of kvservice instance running, including master and slave
port: The port of kvservice instance running
Key: The key to peek
size: Number of elements to peek
```

### Insert data
Kvservice don't support batch insert
#### Use Post
```
curl --location --request POST 'http://{{host}}:{{port}}/api/list/insert' \
--header 'Content-Type: application/json' \
--data-raw '{
    "key": "Key",
    "values": [value1,value2,value3],
    "type": type_string
}'

key: The key of list
values: The array of values with type type_string to insert
type_string: The string of type corresponding to the supported type of Kvservice, if type_string not set, the string data type will be set
```
