//
// Copyright (c) 2025, Novant LLC
// Licensed under the MIT License
//
// History:
//   15 Jan 2025  Andy Frank  Creation
//

using concurrent
using inet

**
** Redis client connection pool.
**
internal const class RedisConnPool
{
  ** Constructor.
  new make(Str host, Int port, Int maxConns)
  {
    this.host = host
    this.port = port
    this.maxConns = maxConns
    this.lock = Lock.makeReentrant
    this.map  = ConcurrentMap()
  }

  ** Allocate a 'RedisConn' inside and pass to given function,
  ** where the connection is released when func completes.
  Obj? exec(|RedisConn->Obj?| func)
  {
    conn := acquire(10sec)
    try
    {
      // yield
      return func(conn)
    }
    catch (Err err)
    {
      // only RedisClient code execs in ths block; so assume a
      // protocol or network error if thrown; close the conn
      // and the next `acquire` call will handle cleanup
      conn.close
      throw err
    }
    finally { release(conn) }
  }

  ** Close all connections in pool.
  Void close()
  {
    doLock(10sec) |->|
    {
      map.each |RedisConn c| { c.close }
      map.clear
    }
  }

  ** Acquire a new connection instance.
  private RedisConn acquire(Duration timeout)
  {
    // start before we try to aquire lock to include
    // that time with waiting for an open connection
    start := Duration.now
    return doLock(timeout) |->Obj?|
    {
      // loop until conn found or timeout is reached
      while (Duration.now - start < timeout)
      {
        for (i:=0; i<maxConns; i++)
        {
          // check for existing open conn
          RedisConn? c
          u := map[i] as Unsafe
          if (u != null)
          {
            c = u.val
            if (c.isClosed) c = null
          }

          // allocate a new instance if null
          if (c == null)
          {
            c = RedisConn.open(host, port)
            map[i] = Unsafe(c)
          }

          // if free then lock and return
          if (!c.inuse.val)
          {
            c.inuse.val = true
            return c
          }
        }

        // short block and then try again
        Actor.sleep(10ms)
      }

      // bail with timeout
      throw errTimeout
    }
  }

  ** Release given connectoin instance.
  private Void release(RedisConn conn)
  {
    // release outside of lock
    conn.inuse.val = false
  }

  ** Evalute callback inside lock.
  private Obj? doLock(Duration timeout, |->Obj?| func)
  {
    if (!lock.tryLock(timeout)) throw errTimeout
    try { return func() }
    finally { lock.unlock }
  }

  private Err errTimeout() { IOErr("Timed out waiting for redis conn") }

  private const Str host
  private const Int port
  private const Int maxConns

  private const Lock lock
  private const ConcurrentMap map
}