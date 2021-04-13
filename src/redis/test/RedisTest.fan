//
// Copyright (c) 2021, Novant LLC
// Licensed under the MIT License
//
// History:
//   11 Apr 2021  Andy Frank  Creation
//

using util
using web

*************************************************************************
** RedisTest
*************************************************************************

class RedisTest : Test
{
  Void testBasics()
  {
    r := Redis.open("localhost")

    v := r.invoke(["SET", "foo", 5])
    echo("$v [$v.typeof]")

    v = r.invoke(["GET", "foo"])
    echo("$v [$v.typeof]")

    v = r.invoke(["INCRBY", "foo", 12])
    echo("$v [$v.typeof]")

    v = r.get("foo")
    echo("$v [$v.typeof]")

    r.del("foo")
    v = r.get("foo")
    echo("? $v")

    echo("---")

    v = r.get("bar")
    echo("$v")

    echo("---")
    r.set("bar", "cool")
    v = r.get("bar")
    echo("$v [$v.typeof]")
  }
}