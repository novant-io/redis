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