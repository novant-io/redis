# Changelog

## Version 0.9 (working)
* Fix `mget` to handle empty keys

## Version 0.8 (13-Dec-2025)
* Add `mget`
* Add `mset`

## Version 0.7 (17-Mon-2024)
* Redesign to use connection pool

## Version 0.6 (6-Feb-2025)
* Add `append`
* Add basic Set support: `sadd`, `srem`, `scard`, `sismember`, `smembers`
* Fix bulk strings to support binary safe read/write

## Version 0.5 (8-Oct-2024)
* Normalize all redis op methods to lowercase for consistency
* Add `expires` support to: `incr`, `incrby`, `incrbyfloat`

## Version 0.4 (6-Aug-2024)
* Allow `null` values on `set` as a convenience for `del`
* Add incr support: `incr`, `incrby`, `incrbyfloat`
* Add hincr support: `hincr`, `hincrby`, `hincrbyfloat`

## Version 0.3 (11-Jan-2024)
* Add basic Hashes support: `hget`, `hmget`, `hgetall`, `hset`, `hmset`, `hdel`
* Add `expire` and `expireAt` calls
* Add `memStats` support

## Version 0.2 (8-Jan-2023)
* Rework `Redis` -> `RedisClient` and make thread-safe
* Update `SocketOptions` -> `SocketConfig`

## Version 0.1 (13-Apr-2021)
* Initial release