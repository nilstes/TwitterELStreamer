# TwitterELStreamer
Streams Twitter messages into ElasticSearch

Run like this:
```
java -jar twitter-el-streamer-1.0-jar-with-dependencies.jar  <twitter-topic> <es-index-name> <es-document-type>
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
host=localhost
port=9200
pipeline=sentiment
```
If file is not present then the above values are used as default, except pipeline which is not defined.
