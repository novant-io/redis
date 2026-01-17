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
    r.set("xa", 71)
    r.set("xb", 72)
    // xc=null

    b := RedisBatch()
      .set("a",    7)
      .setnx("b",  8)
      .setnx("c",  9)
      .set("xa",   77, 500ms)
      .setnx("xb", 78, 500ms)
      .setnx("xc", 79, 500ms)

    v := r.pipeline(b)
    verifyEq(v.size, b.size)
    verifyEq(r.get("a"),  "7")   // set
    verifyEq(r.get("b"),  "2")   // setnx (exists)
    verifyEq(r.get("c"),  "9")   // setnx (not exist)
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
}