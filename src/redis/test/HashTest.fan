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
  }
}