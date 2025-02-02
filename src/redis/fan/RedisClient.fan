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
  new make(Str host, Int port := 6379)
  {
    this.host = host
    this.port = port
  }

  ** Host name of Redis server.
  const Str host

  ** Port number of Redis server.
  const Int port

  ** Close this client all connections if applicable.
  Void close()
  {
    actor.pool.stop.join
  }

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

  ** Delete the given key value.
  Void del(Str key)
  {
    invoke(["DEL", key])
  }

  ** Invoke the given command and return response.
  Obj? invoke(Obj[] args)
  {
    // NOTE: we use unsafe on returns since we can guaretee the
    // reference is not touched again; we also use Unsafe for
    // args for performance to avoid serialization; and in _most_
    // cases this should be fine; but it does create an edge case

    Unsafe u := actor.send(RMsg('v', args)).get
    return u.val
  }

  ** Pipeline multiple `invoke` requests and return batched results.
  Obj?[] pipeline(Obj[] invokes)
  {
    // NOTE: we use unsafe on returns since we can guaretee the
    // reference is not touched again; we also use Unsafe for
    // args for performance to avoid serialization; and in _most_
    // cases this should be fine; but it does create an edge case

    Unsafe u := actor.send(RMsg('p', invokes)).get
    return u.val
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
  ** If 'px' is non-null expire this key after the given timeout
  ** in milliseconds. Returns the value of the key after the increment.
  Int incr(Str key, Duration? px := null)
  {
    if (px == null)
    {
      return invoke(["INCR", key])
    }
    else
    {
      // TODO FIXIT: use MUTLI transaction
      return pipeline([
        ["INCR", key],
        ["PEXPIRE", key, toMillis(px)],
      ]).first
    }
  }

  ** Increments the number stored at key by 'delta'. If the key
  ** does not exist, it is set to 0 before performing the operation.
  ** If 'px' is non-null expire this key after the given timeout
  ** in milliseconds. Returns the value of the key after the increment.
  Int incrby(Str key, Int delta, Duration? px := null)
  {
    if (px == null)
    {
      return invoke(["INCRBY", key, delta])
    }
    else
    {
      // TODO FIXIT: use MUTLI transaction
      return pipeline([
        ["INCRBY", key, delta],
        ["PEXPIRE", key, toMillis(px)],
      ]).first
    }
  }

  ** Increment the string representing a floating point number
  ** stored at 'key' by the specified 'delta'. If the key does not
  ** exist, it is set to 0 before performing the operation. If 'px'
  ** is non-null expire this key after the given timeout in
  ** milliseconds. Returns the value of the key after the increment.
  Float incrbyfloat(Str key, Float delta, Duration? px := null)
  {
    if (px == null)
    {
      Str res := invoke(["INCRBYFLOAT", key, delta])
      return res.toFloat
    }
    else
    {
      // TODO FIXIT: use MUTLI transaction
      Str res := pipeline([
        ["INCRBYFLOAT", key, delta],
        ["PEXPIRE", key, toMillis(px)],
      ]).first
      return res.toFloat
    }
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

//////////////////////////////////////////////////////////////////////////
// Actor
//////////////////////////////////////////////////////////////////////////

  // Actor
  private const ActorPool pool := ActorPool { name="RedisClient" }
  private const Actor actor := Actor(pool) |msg|
  {
    RedisConn? c
    try
    {
      c = Actor.locals["c"]
      if (c == null) Actor.locals["c"] = c = RedisConn(host, port)

      RMsg m := msg
      switch (m.op)
      {
        case 'v': return Unsafe(c.invoke(m.a))
        case 'p': return Unsafe(c.pipeline(m.a))
        default: throw ArgErr("Unknown op '${m.op.toChar}'")
      }
    }
    catch (Err err)
    {
      // TODO: this could be smarter; only teardown for network errs?
      log.err("Unexpected error", err)
      c?.close
      Actor.locals["c"] = null
      throw err
    }
    return null
  }
}

**************************************************************************
** RMsg
**************************************************************************

internal const class RMsg
{
  new make(Int op, Obj? a := null)
  {
    this.op = op
    this.ua = Unsafe(a)
  }

  const Int op
  Obj? a() { ua.val }

  private const Unsafe? ua
}
