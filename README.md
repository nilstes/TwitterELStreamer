# TwitterELStreamer
Streams Twitter messages into ElasticSearch

Run like this:
```
java -jar twitter-el-streamer-1.0-jar-with-dependencies.jar  <twitter-topic> <es-index-name> <es-index-type>
```

twitter4j.properties must be in the current directory with these properties:
```
debug=true
oauth.consumerKey=*********************
oauth.consumerSecret=******************************************
oauth.accessToken=**************************************************
oauth.accessTokenSecret=******************************************
```
es.properties can also be in the current directory with these properties:
```
cluster.name=elasticsearch
host=localhost
port=9300
```
If file is not present then the above values are used as default.
