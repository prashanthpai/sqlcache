# example

```sh
ppai@mbp:~/src/sqlcache/example$ go build .
ppai@mbp:~/src/sqlcache/example$ ./example
i=0; t=2.796458ms
i=1; t=536.167µs
i=2; t=242.125µs
i=3; t=434.375µs
i=4; t=273.666µs
i=5; t=1.674375ms
i=6; t=281.083µs
i=7; t=166.167µs
i=8; t=240.542µs
i=9; t=211.375µs
i=10; t=1.464042ms
i=11; t=381.25µs
i=12; t=293.459µs
i=13; t=284.708µs
i=14; t=240.75µs

Interceptor metrics: &{Hits:12 Misses:3 Errors:0}
```
