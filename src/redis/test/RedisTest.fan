//
// Copyright (c) 2021, Novant LLC
// Licensed under the MIT License
//
// History:
//   11 Apr 2021  Andy Frank  Creation
//

using concurrent

*************************************************************************
** RedisTest
*************************************************************************

class RedisTest : Test
{
  private TestServer? server

  ** Start local test redis-server proc.
  Void startServer()
  {
    server = TestServer()
    server.start(this.tempDir)
    Actor.sleep(500ms)
  }

  ** Teardown test redis-server proc.
  override Void teardown()
  {
    server?.stop
  }

  ** Test basics operations against server.
  Void testBasics()
  {
    startServer
    r := RedisClient(server.host, server.port)
    verifyEq(r.get("foo"), null)
    r.set("foo", 5)
    verifyEq(r.get("foo"), "5")
    r.invoke(["INCRBY", "foo", 3])
    verifyEq(r.get("foo"), "8")
  }

  ** Test basics operations against server.
  Void testPipeline()
  {
    startServer
    r := RedisClient(server.host, server.port)
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
}