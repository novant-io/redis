//
// Copyright (c) 2025, Novant LLC
// All Rights Reserved
//
// History:
//   15 Feb 2025  Andy Frank  Creation
//

using concurrent

*************************************************************************
** PoolTest
*************************************************************************

@NoDoc class PoolTest : AbstractRedisTest
{

//////////////////////////////////////////////////////////////////////////
// Acquire
//////////////////////////////////////////////////////////////////////////

  Void testAcquire()
  {
    conns := 5
    startServer
    r := makeClient(conns)

    // veify no conns
    verifyEq(r->pool->map->size, 0)

    // create actors (conns * 2)
    apool  := ActorPool()
    actors := Actor[,]
    (conns * 2).times |i| {
      actors.add(Actor(apool) |v|
      {
        try
        {
          ActorMsg m := v
          xr := (RedisClient)m.a
          xk := (Str)m.b
          xh := (Duration)m.c
          xr._testHold(xk, xh)
        }
        catch (Err err) err.trace
        return null
      })
    }

    // iterate a few times to aquire/free conns
    3.times {
      actors.each |a,i| {
        a.send(ActorMsg("x", r, "v${i}", 1sec))
      }
    }

    // wait for messages to propogate
    echo("waiting (10sec)...")
    Actor.sleep(10sec)

    // verify redis vals
    actors.each |a,i| { verifyEq(r.get("v${i}"), "3") }

    // verify we can backing conns and inuse == false
    ConcurrentMap map := r->pool->map
    verifyEq(map.size, conns)
    conns.times |i| { verifyEq(map.get(i)->val->inuse->val, false) }
  }

//////////////////////////////////////////////////////////////////////////
// Reopen
//////////////////////////////////////////////////////////////////////////

  Void testReopen()
  {
    conns := 5
    startServer
    r := makeClient(conns)

    // create enough actors to fully populate pool
    apool  := ActorPool()
    actors := Actor[,]
    conns.times |i| {
      actors.add(Actor(apool) |v|
      {
        // try
        // {
          ActorMsg m := v
          xr := (RedisClient)m.a
          if (m.id == "hold") xr._testHold(m.b, m.c)
          if (m.id == "err")  xr._testErr(m.b)
        // }
        // catch (Err err) err.trace
        return null
      })
    }

    // send one message to create connection
    actors.each |a,i| {
      a.send(ActorMsg("hold", r, "v${i}", 1sec))
    }
    echo("waiting (10sec)...")
    Actor.sleep(10sec)

    // verify # conns
    ConcurrentMap map :=r->pool->map
    verifyEq(map.size, conns)
    actors.each |a,i| { verifyEq(r.get("v${i}"), "1") }

    // run thru again and force err
    actors.each |a,i| { a.send(ActorMsg("err", r, 1sec)) }
    echo("waiting (10sec)...")
    Actor.sleep(10sec)

    // verify all closed
    map = r->pool->map
    map.each |u| {
      verifyEq(((RedisConn)u->val).isClosed, true)
    }

    // send one more message to create connection
    actors.each |a,i| {
      a.send(ActorMsg("hold", r, "v${i}", 1sec))
    }
    echo("waiting (10sec)...")
    Actor.sleep(10sec)

    // verify redis vals
    actors.each |a,i| { verifyEq(r.get("v${i}"), "2") }
  }
}