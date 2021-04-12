# Redis Client API for Fantom

Native [Fantom](https://fantom.org) API for [Redis](https://redis.io).

Usage:

```fantom
r := Redis.open("localhost")
r.set("foo", 5)
v := r.get("foo")  // 5

r.invoke(["INCRBY", "foo", 12])
v = r.get("foo")  // 17

r.close
```