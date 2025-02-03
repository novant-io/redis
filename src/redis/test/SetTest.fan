//
// Copyright (c) 2025, Novant LLC
// All Rights Reserved
//
// History:
//   3 Feb 2025  Andy Frank  Creation
//

using concurrent

*************************************************************************
** SetTest
*************************************************************************

@NoDoc class SetTest : AbstractRedisTest
{
  Void test()
  {
    startServer
    r := makeClient

    // verify does not exist
    verifyEq(r.get("foo"), null)
    verifyEq(r.scard("foo"), 0)
    verifyEq(r.smembers("foo"), [,])

    // sadd
    r.sadd("foo", "a")
    verifyEq(r.scard("foo"), 1)
    verifyEq(r.smembers("foo").sort, Obj?["a"])

    // sadd
    r.sadd("foo", "b")
    verifyEq(r.scard("foo"), 2)
    verifyEq(r.smembers("foo").sort, Obj?["a", "b"])

    // sadd (dup)
    r.sadd("foo", "a")
    verifyEq(r.scard("foo"), 2)
    verifyEq(r.smembers("foo").sort, Obj?["a", "b"])

    // sadd (int)
    r.sadd("foo", 5)
    verifyEq(r.scard("foo"), 3)
    verifyEq(r.smembers("foo").sort, Obj?["5", "a", "b"])

    // saddAll
    r.saddAll("foo", ["c", "d", 8])
    verifyEq(r.scard("foo"), 6)
    verifyEq(r.smembers("foo").sort, Obj?["5", "8", "a", "b", "c", "d"])

    // sismember
    verifyEq(r.sismember("foo", "a"), true)
    verifyEq(r.sismember("foo", "b"), true)
    verifyEq(r.sismember("foo", "x"), false)
    verifyEq(r.sismember("foo", 7),   false)

    // srem
    r.srem("foo", "a")
    verifyEq(r.scard("foo"), 5)
    verifyEq(r.smembers("foo").sort, Obj?["5", "8", "b", "c", "d"])

    // srem (noop)
    r.srem("foo", "a")
    verifyEq(r.scard("foo"), 5)
    verifyEq(r.smembers("foo").sort, Obj?["5", "8", "b", "c", "d"])

    // sremall
    r.sremAll("foo", [5, "d", 8])
    verifyEq(r.scard("foo"), 2)
    verifyEq(r.smembers("foo").sort, Obj?["b", "c"])
  }
}