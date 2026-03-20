//
// Copyright (c) 2026, Novant LLC
// Licensed under the MIT License
//
// History:
//   17 Jan 2026  Andy Frank  Creation
//

using concurrent

*************************************************************************
** BatchTest
*************************************************************************

@NoDoc class BatchTest : AbstractRedisTest
{

//////////////////////////////////////////////////////////////////////////
// Get
//////////////////////////////////////////////////////////////////////////

  Void testGet()
  {
    startServer
    r := makeClient
    r.set("foo", 10)

    b := RedisBatch()
      .get("foo")
      .get("bar")
    v := r.pipeline(b)
    verifyEq(v.size, b.size)
    verifyEq(v[0], "10")
    verifyEq(v[1], null)
  }

//////////////////////////////////////////////////////////////////////////
// Set
//////////////////////////////////////////////////////////////////////////

  Void testSet()
  {
    startServer
    r := makeClient
    r.set("a",  1)
    r.set("b",  2)
    // c= null
    r.set("d",  4)
    r.set("xa", 71)
    r.set("xb", 72)
    // xc=null

    b := RedisBatch()
      .set("a",    7)
      .setnx("b",  8)
      .setnx("c",  9)
      .set("d",    null)
      .set("xa",   77, 500ms)
      .setnx("xb", 78, 500ms)
      .setnx("xc", 79, 500ms)

    v := r.pipeline(b)
    verifyEq(v.size, b.size)
    verifyEq(r.get("a"),  "7")   // set
    verifyEq(r.get("b"),  "2")   // setnx (exists)
    verifyEq(r.get("c"),  "9")   // setnx (not exist)
    verifyEq(r.get("d"),  null)  // null -> delete
    verifyEq(r.get("xa"), "77")  // set
    verifyEq(r.get("xb"), "72")  // setnx (exists)
    verifyEq(r.get("xc"), "79")  // setnx (not exist)

    Actor.sleep(600ms)
    verifyEq(r.get("xa"), null)
    verifyEq(r.get("xb"), "72")  // exists so not expired?
    verifyEq(r.get("xc"), null)
  }

//////////////////////////////////////////////////////////////////////////
// Del
//////////////////////////////////////////////////////////////////////////

  Void testDel()
  {
    startServer
    r := makeClient
    r.set("a", 3)
    r.set("b", 5)
    r.set("c", 7)

    b := RedisBatch()
      .del("a")
      .del("c")

    v := r.pipeline(b)
    verifyEq(v.size, b.size)
    verifyEq(r.get("a"), null)
    verifyEq(r.get("b"), "5")
    verifyEq(r.get("c"), null)
  }

//////////////////////////////////////////////////////////////////////////
// Incr
//////////////////////////////////////////////////////////////////////////

  Void testIncr()
  {
    startServer
    r := makeClient
    r.set("a", 3)
    // c=null
    r.set("c", 4)
    // d=null
    r.set("e", 5)
    // f=null

    b := RedisBatch()
      .incr("a")                // exist
      .incr("b")                // not exist
      .incrby("c", 3)           // exist
      .incrby("d", 3)           // not exist
      .incrbyfloat("e", 0.25f)  // exist
      .incrbyfloat("f", 0.25f)  // not exist

    v := r.pipeline(b)
    verifyEq(v.size, b.size)
    verifyEq(r.get("a"), "4")
    verifyEq(r.get("b"), "1")
    verifyEq(r.get("c"), "7")
    verifyEq(r.get("d"), "3")
    verifyEq(r.get("e"), "5.25")
    verifyEq(r.get("f"), "0.25")
  }

//////////////////////////////////////////////////////////////////////////
// Lists
//////////////////////////////////////////////////////////////////////////

  Void testLists()
  {
    startServer
    r := makeClient

    // lpush adds to head, rpush adds to tail
    b := RedisBatch()
      .lpush("foo", "c")
      .lpush("foo", "b")
      .lpush("foo", "a")
      .rpush("foo", "d")
      .rpush("foo", "e")
    v := r.pipeline(b)
    verifyEq(v.size, b.size)

    // list should be [a, b, c, d, e]
    all := r.lrange("foo", 0, -1)
    verifyEq(all.size, 5)
    verifyEq(all[0], "a")
    verifyEq(all[1], "b")
    verifyEq(all[2], "c")
    verifyEq(all[3], "d")
    verifyEq(all[4], "e")

    // lrange subset
    sub := r.lrange("foo", 1, 3)
    verifyEq(sub.size, 3)
    verifyEq(sub[0], "b")
    verifyEq(sub[1], "c")
    verifyEq(sub[2], "d")

    // ltrim to keep first 3
    r.ltrim("foo", 0, 2)
    trimmed := r.lrange("foo", 0, -1)
    verifyEq(trimmed.size, 3)
    verifyEq(trimmed[0], "a")
    verifyEq(trimmed[1], "b")
    verifyEq(trimmed[2], "c")

    // batch lpush + ltrim (capped list pattern)
    b2 := RedisBatch()
      .lpush("capped", "x")
      .lpush("capped", "y")
      .lpush("capped", "z")
      .ltrim("capped", 0, 1)
    r.pipeline(b2)
    capped := r.lrange("capped", 0, -1)
    verifyEq(capped.size, 2)
    verifyEq(capped[0], "z")
    verifyEq(capped[1], "y")
  }
}