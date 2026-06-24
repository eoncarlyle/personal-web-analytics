# Learning Log

## 2026.06.23

Logging done, I've discovered that there is a SQLite module for Grafana which may tick a lot of boxes. Will need to be 
careful ops wise, and I'm still not sure the best way to run timed tasks in Cats world

## 2026.06.22

I need more logging on errors (and just in general, frankly) but what I have works. After that stage, the next piece is 
backups 

## 2026.06.21

For logs like these, we will get a `DecodingFailure at .server_name: Missing required field`, so I will need to figure
out the idiomatic way to deserialise to an `Either` in `fs2-kafka`. I'm not sure if I really need the higher-kinded 
action in `kafkaValueDeserialiser`.

```json
{
  "@timestamp": "2025-12-23T06:13:30.943Z",
  "@metadata": {
    "beat": "filebeat",
    "type": "_doc",
    "version": "8.13.0"
  },
  "agent": {
    "ephemeral_id": "e59ea781-2731-463f-874d-668e9f5eddcc",
    "id": "1535cf8d-15f7-4d49-91bb-dcf5674c47a5",
    "name": "new-york-0",
    "type": "filebeat",
    "version": "8.13.0"
  },
  "log": {
    "offset": 1508,
    "file": {
      "path": "/var/log/nginx/error.log"
    }
  },
  "error": {
    "message": "Error decoding JSON: json: cannot unmarshal number into Go value of type map[string]interface {}",
    "type": "json"
  },
  "input": {
    "type": "log"
  },
  "environment": "production",
  "message": "2025/12/23 06:13:30 [crit] 3164004#3164004: *163131 SSL_do_handshake() failed (SSL: error:0A00006C:SSL routines::bad key share) while SSL handshaking, client: 65.49.1.81, server: 0.0.0.0:443",
  "service": "nginx",
  "ecs": {
    "version": "8.0.0"
  },
  "host": {
    "name": "new-york-0"
  }
}
```


```sqlite
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
PRAGMA cache_size = -20000;
PRAGMA foreign_keys = ON;
```

```sqlite
CREATE TABLE nginx_log (
    server_name TEXT    NOT NULL,
    uri         TEXT    NOT NULL,
    remote_addr TEXT    NOT NULL,
    referrer    TEXT    NOT NULL DEFAULT '',
    count       INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (server_name, uri, remote_addr, referrer)
);
```



## 2026.06.20

This did not work, because the `IO` in `chunk.map` wasn't actually applied. Now it is time to actually understand Cats a
little bit better.

```scala
  private def consumeChunk(chunk: Chunk[ConsumerRecord[Option[String], String]]): IO[CommitNow] = {
  chunk.map(record => IO.println(s"key=${record.value}"))
  IO(CommitNow)
}
```

Okay - after doing some reading I was too worried about the operators (they are just functions) and now have a better
understanding of where Cats ends and Cats Effect ends. But I had an epiphany: _I need to actually use `flatMap`_
(you know, kinda the point of `IO`).


From the [FS2 Kafka Docs](https://typelevel.org/fs2-kafka/docs/consumers#record-streaming):
> When processing of records is independent of each other, as is the case with processRecord above, it's often easier
> and more performant to use stream and mapAsync, as seen in the example below. Generally, it's crucial to ensure there
> are no data races between processing of any two records.
