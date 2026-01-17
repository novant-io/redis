//
// Copyright (c) 2023, Novant LLC
// Licensed under the MIT License
//
// History:
//   8 Jan 2023  Andy Frank  Creation
//

using concurrent
using inet

**************************************************************************
** RedisClient
**************************************************************************

** Redis client.
const class RedisClient
{

//////////////////////////////////////////////////////////////////////////
// Lifecycle
//////////////////////////////////////////////////////////////////////////

  ** Create a new client instance for given host and port.
  new make(Str host, |This|? f := null)
  {
    this.host = host
    if (f != null) f(this)
    this.pool = RedisConnPool(host, port, maxConns)
  }

  ** Host name of Redis server.
  const Str host

  ** Port number of Redis server.
  const Int port := 6379

  ** Max number of simultaneous connections to allow before
  ** blocking calling thread.
  const Int maxConns := 10

  ** Close this client all connections if applicable.
  Void close()
  {
    pool.close
  }

  ** Connection pool.
  private const RedisConnPool pool

  ** Log instance.
  internal const Log log := Log("redis", false)

//////////////////////////////////////////////////////////////////////////
// Base API
//////////////////////////////////////////////////////////////////////////

  ** Get the value for given key.
  Str? get(Str key)
  {
    invoke(["GET", key])
  }

  ** Get the values of all specified keys, or 'null' if the a
  ** given key does not exist or hold a string value.
  Str?[] mget(Str[] keys)
  {
    // short-circuit if nothing provided
    if (keys.isEmpty) return Str?#.emptyList
    return invoke(["MGET"].addAll(keys))
  }

  ** Set the given key to value, if 'val' is null this method deletes
  ** the given key (see `del`). If 'px' is non-null expire this key
  ** after the given timeout in milliseconds.
  Void set(Str key, Obj? val, Duration? px := null)
  {
    // delete if key 'null'
    if (val == null) return del(key)

    // else set
    req := ["SET", key, val]
    if (px != null) req.add("PX").add(toMillis(px))
    invoke(req)
  }

  ** Set the given key to value only if key does not exist. Returns
  ** 'true' if set was succesfull, or false if set failed due to
  ** already existing key.  If 'px' is non-null expire this key after
  ** the given timeout in milliseconds.
  Bool setnx(Str key, Obj val, Duration? px := null)
  {
    req := ["SET", key, val, "NX"]
    if (px != null) req.add("PX").add(toMillis(px))
    return invoke(req) != null
  }

  ** Set multiple key values.
  Void mset(Str:Obj vals)
  {
    req := Obj["MSET"]
    vals.each |v,k|
    {
      req.add(k)
      req.add(v)
    }
    invoke(req)
  }

  ** Delete the given key value.
  Void del(Str key)
  {
    invoke(["DEL", key])
  }

  ** Invoke the given command and return response.
  Obj? invoke(Obj[] args)
  {
    pool.exec |conn| { conn.invoke(args) }
  }

  ** Pipeline multiple `invoke` requests and return batched results.
  Obj?[] pipeline(RedisBatch batch)
  {
    pool.exec |conn| { conn.pipeline(batch.cmds) }
  }

  ** Execute batch commands atomically in a MULTI/EXEC transaction.
  ** Returns array of results in same order as batch commands.
  Obj?[]? multi(RedisBatch batch)
  {
    pool.exec |conn| { conn.multi(batch.cmds) }
  }

//////////////////////////////////////////////////////////////////////////
// Expire
//////////////////////////////////////////////////////////////////////////

  ** Expire given key after given 'seconds' has elasped, where
  ** timeout must be in even second intervals.
  Void expire(Str key, Duration seconds)
  {
    sec := toSec(seconds)
    invoke(["EXPIRE", key, sec])
  }

  ** Expire given key when the given 'timestamp' has been reached,
  ** where 'timestamp' has a resolution of whole seconds.
  Void expireat(Str key, DateTime timestamp)
  {
    unix := timestamp.toJava / 1000
    invoke(["EXPIREAT", key, unix])
  }

  ** Expire given key after given 'ms' has elasped, where
  ** timeout must be in even millisecond intervals.
  Void pexpire(Str key, Duration milliseconds)
  {
    ms := toMillis(milliseconds)
    invoke(["PEXPIRE", key, ms])
  }

//////////////////////////////////////////////////////////////////////////
// Incr
//////////////////////////////////////////////////////////////////////////

  ** Increments the number stored at key by one. If the key does
  ** not exist, it is set to 0 before performing the operation.
  ** Returns the value of the key after the increment.
  Int incr(Str key)
  {
    invoke(["INCR", key])
  }

  ** Increments the number stored at key by 'delta'. If the key
  ** does not exist, it is set to 0 before performing the operation.
  ** Returns the value of the key after the increment.
  Int incrby(Str key, Int delta)
  {
    invoke(["INCRBY", key, delta])
  }

  ** Increment the string representing a floating point number
  ** stored at 'key' by the specified 'delta'. If the key does not
  ** exist, it is set to 0 before performing the operation. Returns
  ** the value of the key after the increment.
  Float incrbyfloat(Str key, Float delta)
  {
    Str res := invoke(["INCRBYFLOAT", key, delta])
    return res.toFloat
  }

//////////////////////////////////////////////////////////////////////////
// Append
//////////////////////////////////////////////////////////////////////////

  ** If 'key' already exists and is a string, this command appends
  ** the value at the end of the string. If 'key' does not exist
  ** it is created and set as an empty string, so 'append' will be
  ** similar to `set` in this special case.
  Void append(Str key, Obj val)
  {
    invoke(["APPEND", key, val])
  }

//////////////////////////////////////////////////////////////////////////
// Sets
//////////////////////////////////////////////////////////////////////////

  ** Add the specified member to the set stored at key, or ignore
  ** if this member already exists in this set. If key does not exist,
  ** a new set is created before adding the specified members.
  Void sadd(Str key, Obj member)
  {
    invoke(["SADD", key, member])
  }

  ** Add the specified members to the set stored at key. Specified
  ** members that are already a member of this set are ignored. If
  ** key does not exist, a new set is created before adding the
  ** specified members.
  Void saddAll(Str key, Obj[] members)
  {
    invoke(Obj["SADD", key].addAll(members))
  }

  ** Remove the specified member from the set stored at key, or do
  ** nothing if member does not exist in this set.  If key does not
  ** exist, this method does nothing.
  Void srem(Str key, Obj member)
  {
    invoke(["SREM", key, member])
  }

  ** Remove the specified member from the set stored at key. Specified
  ** members that are not a member of this set are ignored. If key does
  ** not exist, this method does nothing.
  Void sremAll(Str key, Obj[] members)
  {
    invoke(Obj["SREM", key].addAll(members))
  }

  ** Returns the set cardinality (number of elements) of the set
  ** stored at key.
  Int scard(Str key)
  {
    invoke(["SCARD", key])
  }

  ** Returns 'true' if member is a member of the set stored at key.
  Bool sismember(Str key, Obj member)
  {
    invoke(["SISMEMBER", key, member]) == 1
  }

  ** Returns all the members of the set value stored at key.
  Str[] smembers(Str key)
  {
    invoke(["SMEMBERS", key])
  }

  // `sadd`, `srem`, `scard`, `sismember`, `smembers`

//////////////////////////////////////////////////////////////////////////
// Hash
//////////////////////////////////////////////////////////////////////////

  ** Get the hash field for given key.
  Str? hget(Str key, Str field)
  {
    invoke(["HGET", key, field])
  }

  ** Get the hash field for given key.
  Str?[] hmget(Str key, Str[] fields)
  {
    invoke(["HMGET", key].addAll(fields))
  }

  ** Get all hash field values for given key.
  Str:Str hgetall(Str key)
  {
    map := Str:Str[:]
    List acc := invoke(["HGETALL", key])

    for (i:=0; i<acc.size; i+=2)
    {
      k := acc[i]
      v := acc[i+1]
      map[k] = v
    }

    return map
  }

  ** Set the hash field to the given value for key.
  Void hset(Str key, Str field, Obj val)
  {
    invoke(["HSET", key, field, val])
  }

  ** Set all hash values in 'vals' for given key.
  Void hmset(Str key, Str:Obj vals)
  {
    acc := Obj["HMSET", key]
    vals.each |v,k| { acc.add(k).add(v) }
    invoke(acc)
  }

  ** Delete given hash field for key.
  Void hdel(Str key, Str field)
  {
    invoke(["HDEL", key, field])
  }

  ** Convenience for 'hincrby(key, field 1)'
  Int hincr(Str key, Str field)
  {
    hincrby(key, field, 1)
  }

  ** Increments the number stored at 'field' in the hash stored at
  ** 'key' by given 'delta'. If the field does not exist, it is set
  ** to 0 before performing the operation.
  Int hincrby(Str key, Str field, Int delta)
  {
    invoke(["HINCRBY", key, field, delta])
  }

  ** Increment the string representing a floating point number stored
  ** at 'field' in the hash stored at 'key' by the specified 'delta'.
  ** If the key does not exist, it is set to 0 before performing the
  ** operation.
  Float hincrbyfloat(Str key, Str field, Float delta)
  {
    Str res := invoke(["HINCRBYFLOAT", key, field, delta])
    return res.toFloat
  }

//////////////////////////////////////////////////////////////////////////
// Misc API
//////////////////////////////////////////////////////////////////////////

  ** Returns information about the memory usage of server.
  Str:Obj memStats()
  {
    List acc := invoke(["MEMORY", "STATS"])
    map := Str:Obj[:] { it.ordered=true }
    for (i:=0; i<acc.size; i+=2)
    {
      k := acc[i]
      v := acc[i+1]
      map[k] = v
    }
    return map
  }

//////////////////////////////////////////////////////////////////////////
// Testing
//////////////////////////////////////////////////////////////////////////

  ** This is used for unit testing to verify connection pools.
  internal Void _testHold(Str key, Duration hold)
  {
    // set and then hold conn to test pool exhastion
    pool.exec |conn| {
      conn.invoke(["INCR", key])
      Actor.sleep(hold)
      return null
    }
  }

    ** This is used for unit testing to verify connection pools.
  internal Void _testErr(Duration hold)
  {
    // simulate err to teardown connection
    pool.exec |conn| {
      Actor.sleep(hold)
      throw IOErr("Test error")
    }
  }

//////////////////////////////////////////////////////////////////////////
// Support
//////////////////////////////////////////////////////////////////////////

  ** Convert duration to even millis or throw if < 1ms
  private Int toMillis(Duration d)
  {
    ms := d.toMillis
    if (ms < 1) throw ArgErr("Non-zero timeout in milliseconds required")
    return ms
  }

  ** Convert duration to even millis or throw if < 1ms
  private Int toSec(Duration d)
  {
    sec := d.toSec
    if (sec < 1) throw ArgErr("Non-zero timeout in seconds required")
    return sec
  }
}