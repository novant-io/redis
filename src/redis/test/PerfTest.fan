//
// Copyright (c) 2025, Novant LLC
// All Rights Reserved
//
// History:
//   15 Feb 2025  Andy Frank  Creation
//

using concurrent

*************************************************************************
** PerfTest
*************************************************************************

@NoDoc class PerfTest : AbstractRedisTest
{
  Void testReadStr()
  {
    startServer
    r := makeClient

    // set large value
    v := StrBuf()
    512.times { v.addChar('a') }
    r.set("foo", v.toStr)

    a := Duration.now
    c := 100_000
    c.times
    {
      x := r.get("foo")
    }
    b := Duration.now
    d := b - a
    x := Duration(d.ticks / c)
    echo("# testReadStr [$c.toLocale cycles, ${d.toMillis} ms, ${x.toLocale}/req]")
  }
}