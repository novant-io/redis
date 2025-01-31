//
// Copyright (c) 2021, Novant LLC
// Licensed under the MIT License
//
// History:
//   11 Apr 2021  Andy Frank  Creation
//

using concurrent

*************************************************************************
** BasicTest
*************************************************************************

@NoDoc class BasicTest : AbstractRedisTest
{

//////////////////////////////////////////////////////////////////////////
// Reader/Writer
//////////////////////////////////////////////////////////////////////////

  ** RespReader parser tests.
  Void testReader()
  {
    verifyEq(read("+OK\r\n"), "OK")
    verifyEq(read(":5\r\n"),  5)
    verifyEq(read("\$3\r\nfoo\r\n"),  "foo")
    verifyEq(read("\$-1\r\n"), null)
    verifyEq(
      read("*3\r\n+OK\r\n:500\r\n\$6\r\nfoobar\r\n"),
      Obj?["OK", 500, "foobar"])
    verifyErr(IOErr#) { read("-ERR\r\n") }
  }

  private Obj? read(Str s)
  {
    buf := Buf().print(s)
    return RespReader(buf.flip.in).read
  }

  ** RespWriter parser tests.
  Void testWriter()
  {
    // we always write bulk str
    verifyEq(write("OK"), "\$2\r\nOK\r\n")
    verifyEq(write(5),    "\$1\r\n5\r\n")
    verifyEq(write(null), "\$-1\r\n")
    verifyEq(
      write(["OK",500,"foobar"]),
      "*3\r\n\$2\r\nOK\r\n\$3\r\n500\r\n\$6\r\nfoobar\r\n")
  }

  private Str write(Obj? obj)
  {
    buf := StrBuf()
    RespWriter(buf.out).write(obj)
    return buf.toStr
  }

//////////////////////////////////////////////////////////////////////////
// Basics
//////////////////////////////////////////////////////////////////////////

  ** Test basics operations against server.
  Void testBasics()
  {
    startServer
    r := makeClient

    // verify does not exist
    verifyEq(r.get("foo"), null)

    // set and verify
    r.set("foo", 5)
    verifyEq(r.get("foo"), "5")

    // inc and verify
    r.invoke(["INCRBY", "foo", 3])
    verifyEq(r.get("foo"), "8")

    // del and verify
    r.del("foo")
    verifyEq(r.get("foo"), null)

    // set null
    r.set("bar", null)
    verifyEq(r.get("bar"), null)
    r.set("bar", "xyz")
    verifyEq(r.get("bar"), "xyz")
    r.set("bar", null)
    verifyEq(r.get("bar"), null)

    // setnx
    verifyEq(r.setnx("nxa", 100), true)    // ok
    verifyEq(r.setnx("nxb", 250), true)    // ok
    verifyEq(r.get("nxa"), "100")
    verifyEq(r.get("nxb"), "250")
    // fail: exists
    verifyEq(r.setnx("nxa", -1), false)
    verifyEq(r.get("nxa"), "100")
    // del and ok
    r.del("nxa")
    verifyEq(r.setnx("nxa", -1), true)
    verifyEq(r.get("nxa"), "-1")
  }

//////////////////////////////////////////////////////////////////////////
// Keys
//////////////////////////////////////////////////////////////////////////

  ** Test keys.
  Void testKeys()
  {
    startServer
    r := makeClient

    // programtic strings
    verifyKey(r, "foo",     5)
    verifyKey(r, "foo_bar", 6)
    verifyKey(r, "foo123",  7)

    // spaces
    verifyKey(r, "foo bar",    10)
    verifyKey(r, " foo bar  ", 11)
    verifyKey(r, "   5",       12)

    // chars
    verifyKey(r, "Something/5 Else/1.2.3", 20)
    verifyKey(r, "Something/5 (#55; Else/1.2.3)", 21)
  }

  private Void verifyKey(RedisClient r, Str key, Obj val)
  {
    r.set(key, val)
    verifyEq(r.get(key), val.toStr)

    r.hset("map", key, val)
    verifyEq(r.hget("map", key), val.toStr)
  }

//////////////////////////////////////////////////////////////////////////
// Expires
//////////////////////////////////////////////////////////////////////////

  ** Test expires.
  Void testExpires()
  {
    startServer
    r := makeClient

    // set and expire
    r.set("foo", 5)
    r.expire("foo", 1500ms)
    verifyEq(r.get("foo"), "5")
    Actor.sleep(500ms)
    verifyEq(r.get("foo"), "5")
    Actor.sleep(1100ms)
    verifyEq(r.get("foo"), null)

    // set and expire at
    at := DateTime.now + 1500ms
    r.set("bar", 3)
    r.expireat("bar", at)
    verifyEq(r.get("bar"), "3")
    Actor.sleep(500ms)
    verifyEq(r.get("bar"), "3")
    Actor.sleep(1100ms)
    verifyEq(r.get("bar"), null)

    // set multiple
    r.set("zar", 9)
    r.expire("zar", 1sec)
    Actor.sleep(500ms)
    // extend
    verifyEq(r.get("zar"), "9")
    r.expire("zar", 1sec)
    Actor.sleep(500ms)
    // extend
    verifyEq(r.get("zar"), "9")
    r.expire("zar", 1sec)
    Actor.sleep(1100ms)
    verifyEq(r.get("zar"), null)

    // pexpire
    r.set("pbar", 5)
    r.pexpire("pbar", 100ms)
    verifyEq(r.get("pbar"), "5")
    Actor.sleep(110ms)
    verifyEq(r.get("pbar"), null)

    // set px
    r.set("pxfoo", 10, 100ms)
    verifyEq(r.get("pxfoo"), "10")
    Actor.sleep(110ms)
    verifyEq(r.get("pxfoo"), null)

    // setnx px
    r.setnx("pxbar", 10, 100ms)
    r.setnx("pxbar", 15, 500ms)
    verifyEq(r.get("pxbar"), "10")
    Actor.sleep(110ms)
    verifyEq(r.get("pxbar"), null)
    r.setnx("pxbar", 15, 500ms)
    verifyEq(r.get("pxbar"), "15")

    // errs
    verifyErr(ArgErr#) { r.expire("xxx",  10ms) }  //  < 1sec
    verifyErr(ArgErr#) { r.pexpire("xxx", 10ns) }  //  < 1ms
    verifyErr(ArgErr#) { r.set("xxx", 0,  10ns) }  //  < 1ms
  }

//////////////////////////////////////////////////////////////////////////
// Incr
//////////////////////////////////////////////////////////////////////////

  ** Test incr ops.
  Void testIncr()
  {
    startServer
    r := makeClient

    // does not exist
    verifyEq(r.incr("a"), 1)
    verifyEq(r.incrby("b", 5), 5)
    verifyEq(r.incrbyfloat("c", 0.125f), 0.125f)
    verifyEq(r.get("a"), "1")
    verifyEq(r.get("b"), "5")
    verifyEq(r.get("c"), "0.125")

    // incr again
    verifyEq(r.incr("a"), 2)
    verifyEq(r.incrby("b", 3), 8)
    verifyEq(r.incrbyfloat("c", 1.2f), 1.325f)
    verifyEq(r.get("a"), "2")
    verifyEq(r.get("b"), "8")
    verify(r.get("c").toFloat.approx(1.325f))

    // decr
    verifyEq(r.incrby("b", -4), 4)
    verifyEq(r.incrbyfloat("c", -0.025f), 1.3f)
    verifyEq(r.get("b"), "4")
    verify(r.get("c").toFloat.approx(1.3f))

    // incr + expr
    verifyEq(r.incr("a", 100ms), 3)
    verifyEq(r.incrby("b", 3, 100ms), 7)
    verifyEq(r.incrbyfloat("c", 3.3f, 100ms), 4.6f)
    verifyEq(r.get("a"), "3")
    verifyEq(r.get("b"), "7")
    verify(r.get("c").toFloat.approx(4.6f))
    Actor.sleep(110ms)
    verifyEq(r.get("a"), null)
    verifyEq(r.get("b"), null)
    verifyEq(r.get("c"), null)
  }

//////////////////////////////////////////////////////////////////////////
// Escaping
//////////////////////////////////////////////////////////////////////////

  ** Test basics operations against server.
  Void testEscaping()
  {
    startServer
    r := makeClient

    // $
    r.set("a", "foo\$bar")
    verifyEq(r.get("a"), "foo\$bar")

    // €
    // r.set("b", "€100")
    // verifyEq(r.get("b"), "€100")
  }

//////////////////////////////////////////////////////////////////////////
// Pipeline
//////////////////////////////////////////////////////////////////////////

  ** Test basics operations against server.
  Void testPipeline()
  {
    startServer
    r := makeClient
    v := r.pipeline([
      ["GET",    "foo"],
      ["SET",    "foo", 5],
      ["GET",    "foo"],
      ["INCRBY", "foo", 3],
      ["GET",    "foo"],
    ])
    verifyEq(v.size, 5)
    verifyEq(v[0], null)
    verifyEq(v[1], "OK")
    verifyEq(v[2], "5")
    verifyEq(v[3], 8)
    verifyEq(v[4], "8")
  }
}