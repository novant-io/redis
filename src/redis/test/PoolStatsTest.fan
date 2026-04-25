//
// Copyright (c) 2026, Novant LLC
// Licensed under the MIT License
//
// History:
//   25 Apr 2026  Andy Frank  Creation
//

using concurrent

*************************************************************************
** PoolStatsTest
*************************************************************************

@NoDoc class PoolStatsTest : AbstractRedisTest
{

//////////////////////////////////////////////////////////////////////////
// Unit: Initial
//////////////////////////////////////////////////////////////////////////

  Void testInitial()
  {
    s := RedisPoolStats()
    verifyEq(s.inUse, 0)
    verifyEq(s.maxInUse, 0)
    verifyEq(s.acquireTimeouts, 0)
    verifyEq(s.maxAcquireWait, 0ns)
  }

//////////////////////////////////////////////////////////////////////////
// Unit: Acquire / Release
//////////////////////////////////////////////////////////////////////////

  Void testAcquireRelease()
  {
    s := RedisPoolStats()

    s.onAcquire(5ms.ticks)
    verifyEq(s.inUse, 1)
    verifyEq(s.maxInUse, 1)
    verifyEq(s.maxAcquireWait, 5ms)

    s.onAcquire(3ms.ticks)
    verifyEq(s.inUse, 2)
    verifyEq(s.maxInUse, 2)
    verifyEq(s.maxAcquireWait, 5ms)

    s.onAcquire(20ms.ticks)
    verifyEq(s.inUse, 3)
    verifyEq(s.maxInUse, 3)
    verifyEq(s.maxAcquireWait, 20ms)

    s.onRelease
    s.onRelease
    verifyEq(s.inUse, 1)
    verifyEq(s.maxInUse, 3)
    verifyEq(s.maxAcquireWait, 20ms)
  }

//////////////////////////////////////////////////////////////////////////
// Unit: Timeouts
//////////////////////////////////////////////////////////////////////////

  Void testAcquireTimeout()
  {
    s := RedisPoolStats()
    s.onAcquireTimeout
    s.onAcquireTimeout
    s.onAcquireTimeout
    verifyEq(s.acquireTimeouts, 3)
    verifyEq(s.inUse, 0)
    verifyEq(s.maxInUse, 0)
    verifyEq(s.maxAcquireWait, 0ns)
  }

//////////////////////////////////////////////////////////////////////////
// Unit: Reset
//////////////////////////////////////////////////////////////////////////

  Void testReset()
  {
    s := RedisPoolStats()
    s.onAcquire(50ms.ticks)
    s.onAcquire(100ms.ticks)
    s.onAcquireTimeout
    s.onAcquireTimeout
    s.onRelease  // 1 still in-use

    s.reset

    // gauge preserved; maxInUse reseeded to current inUse
    verifyEq(s.inUse, 1)
    verifyEq(s.maxInUse, 1)
    verifyEq(s.maxAcquireWait, 0ns)
    verifyEq(s.acquireTimeouts, 0)

    // a smaller wait now sets the peak since the window restarted
    s.onAcquire(10ms.ticks)
    verifyEq(s.inUse, 2)
    verifyEq(s.maxInUse, 2)
    verifyEq(s.maxAcquireWait, 10ms)
  }

//////////////////////////////////////////////////////////////////////////
// Integration: gauge tracks real acquires/releases
//////////////////////////////////////////////////////////////////////////

  Void testInUseRoundTrip()
  {
    startServer
    r := makeClient(5)
    s := r.poolStats

    // before any traffic
    verifyEq(s.inUse, 0)
    verifyEq(s.maxInUse, 0)

    // single command — gauge should bump to 1 mid-flight, back to 0 after
    r.set("k", "v")
    verifyEq(s.inUse, 0)
    verifyEq(s.maxInUse, 1)

    // many sequential commands — peak stays at 1, gauge always returns to 0
    20.times { r.incr("k2") }
    verifyEq(s.inUse, 0)
    verifyEq(s.maxInUse, 1)
  }

//////////////////////////////////////////////////////////////////////////
// Integration: maxInUse under concurrent load
//////////////////////////////////////////////////////////////////////////

  Void testMaxInUseUnderLoad()
  {
    conns := 4
    hold  := 300ms
    startServer
    r := makeClient(conns)
    s := r.poolStats

    // launch `conns` actors that each hold a connection concurrently
    apool   := ActorPool()
    futures := Future[,]
    conns.times |i| {
      a := Actor(apool) |msg|
      {
        ActorMsg m := msg
        xr := (RedisClient)m.a
        xk := (Str)m.b
        xh := (Duration)m.c
        xr._testHold(xk, xh)
        return null
      }
      futures.add(a.send(ActorMsg("x", r, "k${i}", hold)))
    }
    futures.each |f| { f.get(10sec) }

    verifyEq(s.inUse, 0)
    verifyEq(s.maxInUse, conns)
  }

//////////////////////////////////////////////////////////////////////////
// Integration: maxAcquireWait records contention
//////////////////////////////////////////////////////////////////////////

  Void testWaitRecorded()
  {
    hold := 400ms
    startServer
    r := makeClient(1)  // single conn -> guaranteed contention
    s := r.poolStats

    // a1 grabs the only conn and holds it
    apool := ActorPool()
    holder := Actor(apool) |msg|
    {
      ActorMsg m := msg
      ((RedisClient)m.a)._testHold((Str)m.b, (Duration)m.c)
      return null
    }
    f1 := holder.send(ActorMsg("x", r, "k1", hold))

    // small delay to ensure a1 has the conn before a2 starts waiting
    Actor.sleep(50ms)

    // a2 has to wait until a1 releases
    waiter := Actor(apool) |msg|
    {
      ((RedisClient)msg).set("k2", "v")
      return null
    }
    f2 := waiter.send(r)

    f1.get(10sec)
    f2.get(10sec)

    // a2 should have waited roughly (hold - 50ms) for a connection.
    // Use a generous lower bound to absorb scheduling jitter.
    verify(s.maxAcquireWait > 100ms,
      "expected maxAcquireWait > 100ms, got ${s.maxAcquireWait}")
    verifyEq(s.inUse, 0)
    verifyEq(s.maxInUse, 1)
    verifyEq(s.acquireTimeouts, 0)
  }

//////////////////////////////////////////////////////////////////////////
// Integration: reset against a live client
//////////////////////////////////////////////////////////////////////////

  Void testResetLive()
  {
    startServer
    r := makeClient(2)
    s := r.poolStats

    r.set("k", "v")
    r.set("k", "v")
    verify(s.maxInUse >= 1)

    s.reset
    verifyEq(s.inUse, 0)
    verifyEq(s.maxInUse, 0)
    verifyEq(s.maxAcquireWait, 0ns)
    verifyEq(s.acquireTimeouts, 0)

    // post-reset traffic re-establishes peak
    r.set("k", "v")
    verifyEq(s.maxInUse, 1)
  }
}
