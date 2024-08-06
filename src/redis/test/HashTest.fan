//
// Copyright (c) 2024, Novant LLC
// All Rights Reserved
//
// History:
//   10 Jan 2024  Andy Frank  Creation
//

using concurrent

*************************************************************************
** HashTest
*************************************************************************

@NoDoc class HashTest : AbstractRedisTest
{
  Void test()
  {
    startServer
    r := makeClient

    // verify does not exist
    verifyEq(r.hget("foo", "a"), null)

    // set and check
    r.hset("foo", "a", 5)
    verifyEq(r.hget("foo", "a"), "5")

    // mset
    r.hmset("foo", ["a":7, "b":3, "c":"wagon"])
    verifyEq(r.hget("foo", "a"), "7")
    verifyEq(r.hget("foo", "b"), "3")
    verifyEq(r.hget("foo", "c"), "wagon")

    // hgetall
    a := r.hgetall("foo")
    verifyEq(a.size, 3)
    verifyEq(a["a"], "7")
    verifyEq(a["b"], "3")
    verifyEq(a["c"], "wagon")

    // hmget
    m := r.hmget("foo", ["a","c"])
    verifyEq(m[0], "7")
    verifyEq(m[1], "wagon")

    // hincr does not exist
    verifyEq(r.hincr("foo", "ia"), 1)
    verifyEq(r.hincrby("foo", "ib", 5), 5)
    verifyEq(r.hincrbyfloat("foo", "ic", 0.125f), 0.125f)
    verifyEq(r.hget("foo", "ia"), "1")
    verifyEq(r.hget("foo", "ib"), "5")
    verifyEq(r.hget("foo", "ic"), "0.125")

    // hincr exists
    verifyEq(r.hincr("foo", "ia"), 2)
    verifyEq(r.hincrby("foo", "ib", 3), 8)
    verifyEq(r.hincrbyfloat("foo", "ic", 1.2f), 1.325f)
    verifyEq(r.hget("foo", "ia"), "2")
    verifyEq(r.hget("foo", "ib"), "8")
    verify(r.hget("foo", "ic").toFloat.approx(1.325f))

    // hincr decrement
    verifyEq(r.hincrby("foo", "ib", -4), 4)
    verifyEq(r.hincrbyfloat("foo", "ic", -0.025f), 1.3f)
    verifyEq(r.hget("foo", "ib"), "4")
    verify(r.hget("foo", "ic").toFloat.approx(1.3f))

    // hdel
    r.hdel("foo", "b")
    a = r.hgetall("foo")
    verifyEq(a["a"], "7")
    verifyEq(a["c"], "wagon")

    // del entire hash
    r.del("foo")
    a = r.hgetall("foo")
    verifyEq(a.size, 0)
  }
}