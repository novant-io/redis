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
  }

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
    r.expireAt("bar", at)
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
  }

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