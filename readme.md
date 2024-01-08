# Redis Client API for Fantom

Native [Fantom](https://fantom.org) API for [Redis](https://redis.io).

Usage:

```fantom
r := RedisClient("localhost")

// get/set/del conveniences
r.set("foo", 5)
v := r.get("foo")  // 5
r.del("foo")

// invoke any cmd
r.invoke(["INCRBY", "foo", 12])
v = r.get("foo")   // 17

// support for pipelining
v = r.pipeline([
  ["SET",    "bar", 5],
  ["INCRBY", "bar", 3],
  ["GET",    "bar"],
])
echo(v[2])  // 8
```