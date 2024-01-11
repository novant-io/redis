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

class HashTest : AbstractRedisTest
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
    verifyEq(a.size, 6)
    verifyEq(a[0], "a")
    verifyEq(a[1], "7")
    verifyEq(a[2], "b")
    verifyEq(a[3], "3")
    verifyEq(a[4], "c")
    verifyEq(a[5], "wagon")

    // hmget
    m := r.hmget("foo", ["a","c"])
    verifyEq(m[0], "7")
    verifyEq(m[1], "wagon")

    // hdel
    r.hdel("foo", "b")
    a = r.hgetall("foo")
    verifyEq(a.size, 4)
    verifyEq(a[0], "a")
    verifyEq(a[1], "7")
    verifyEq(a[2], "c")
    verifyEq(a[3], "wagon")
  }
}