# Changelog

## Version 0.4 (working)
* Allow `null` values on `set` as a convenience for `del`

## Version 0.3 (11-Jan-2024)
* Add basic Hashes support: `hget`, `hmget`, `hgetall`, `hset`, `hmset`, `hdel`
* Add `expire` and `expireAt` calls
* Add `memStats` support

## Version 0.2 (8-Jan-2023)
* Rework `Redis` -> `RedisClient` and make thread-safe
* Update `SocketOptions` -> `SocketConfig`

## Version 0.1 (13-Apr-2021)
* Initial release