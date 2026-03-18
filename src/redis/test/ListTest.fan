//
// Copyright (c) 2026, Novant LLC
// All Rights Reserved
//
// History:
//   18 Mar 2026  Andy Frank  Creation
//

using concurrent

*************************************************************************
** ListTest
*************************************************************************

@NoDoc class ListTest : AbstractRedisTest
{
  Void test()
  {
    startServer
    r := makeClient

    // verify does not exist
    verifyEq(r.llen("foo"), 0)
    verifyEq(r.lrange("foo"), [,])

    // push 1
    r.lpush("foo", 1)
    verifyEq(r.llen("foo"), 1)
    verifyEq(r.lrange("foo"), Obj?["1"])

    // push more
    r.lpush("foo", 2)
    r.lpush("foo", "a")
    verifyEq(r.llen("foo"), 3)
    verifyEq(r.lrange("foo"), Obj?["a", "2", "1"])

    // push list
    r.lpush("foo", ["b",5,6])
    verifyEq(r.llen("foo"), 6)
    verifyEq(r.lrange("foo"), Obj?["6", "5", "b", "a", "2", "1"])

    // range
    verifyEq(r.lrange("foo",  0,  0), Obj?["6"])
    verifyEq(r.lrange("foo",  0,  1), Obj?["6", "5"])
    verifyEq(r.lrange("foo",  2,  4), Obj?["b", "a", "2"])
    verifyEq(r.lrange("foo", -2, -1), Obj?["2", "1"])
    verifyEq(r.lrange("foo", -1, -1), Obj?["1"])

    // trim
    r.ltrim("foo", 0, 3)
    verifyEq(r.llen("foo"), 4)
    verifyEq(r.lrange("foo"), Obj?["6", "5", "b", "a"])
    r.ltrim("foo", 1, 2)
    verifyEq(r.llen("foo"), 2)
    verifyEq(r.lrange("foo"), Obj?["5", "b"])

    // rpush
    r.rpush("foo", 7)
    verifyEq(r.llen("foo"), 3)
    verifyEq(r.lrange("foo"), Obj?["5", "b", "7"])
    r.rpush("foo", [8,9,10])
    verifyEq(r.llen("foo"), 6)
    verifyEq(r.lrange("foo"), Obj?["5", "b", "7", "8", "9", "10"])
  }
}