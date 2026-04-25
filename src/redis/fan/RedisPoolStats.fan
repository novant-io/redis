//
// Copyright (c) 2026, Novant LLC
// Licensed under the MIT License
//
// History:
//   22 Apr 2026  Andy Frank  Creation
//

using concurrent

**************************************************************************
** RedisPoolStats
**************************************************************************

** Redis client pool statistics.
const class RedisPoolStats
{
  ** Current number of in-use connections.
  Int inUse() { inUseRef.val }

  ** Max connections in-use observed since last `reset`.
  Int maxInUse() { maxInUseRef.val }

  ** Total acquire timeouts since last `reset`.
  Int acquireTimeouts() { acquireTimeoutsRef.val }

  ** Max acquire wait time observed since last `reset`.
  Duration maxAcquireWait() { Duration(maxAcquireWaitRef.val) }

  ** Reset peak/counter values. The `inUse` gauge is unaffected;
  ** `maxInUse` is reseeded to the current `inUse`.
  Void reset()
  {
    maxInUseRef.val        = inUseRef.val
    acquireTimeoutsRef.val = 0
    maxAcquireWaitRef.val  = 0
  }

  internal Void onAcquire(Int waitTicks)
  {
    n := inUseRef.incrementAndGet
    updateMax(maxInUseRef, n)
    updateMax(maxAcquireWaitRef, waitTicks)
  }

  internal Void onRelease()        { inUseRef.decrementAndGet }
  internal Void onAcquireTimeout() { acquireTimeoutsRef.incrementAndGet }

  private static Void updateMax(AtomicInt ref, Int v)
  {
    while (true)
    {
      cur := ref.val
      if (v <= cur) return
      if (ref.compareAndSet(cur, v)) return
    }
  }

  private const AtomicInt inUseRef           := AtomicInt(0)
  private const AtomicInt maxInUseRef        := AtomicInt(0)
  private const AtomicInt acquireTimeoutsRef := AtomicInt(0)
  private const AtomicInt maxAcquireWaitRef  := AtomicInt(0)
}
