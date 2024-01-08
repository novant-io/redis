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

  ** Convenience for 'invoke(["GET", key])'.
  Str? get(Str key)
  {
    invoke(["GET", key])
  }

  ** Convenience for 'invoke(["SET", key, val])'.
  Void set(Str key, Obj val)
  {
    invoke(["SET", key, val])
  }

  ** Convenience for 'invoke(["DEL", key])'.
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

  ** Log instance.
  internal const Log log := Log("redis", false)

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