# Learning Log

## 2026.06.20

This did not work, because the `IO` in `chunk.map` wasn't actually applied. Now it is time to actually understand Cats a little bit better.

```scala
  private def consumeChunk(chunk: Chunk[ConsumerRecord[Option[String], String]]): IO[CommitNow] = {
  chunk.map(record => IO.println(s"key=${record.value}"))
  IO(CommitNow)
}
```

Okay - after doing some reading I was too worried about the operators (they are just functions) and now have a better
understanding of where Cats ends and Cats Effect ends. But I had an epiphany: _I need to actually use `flatMap`_
(you know, kinda the point of `IO`).