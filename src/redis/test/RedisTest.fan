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
    verifyEq(
      read("*3\r\n+foo\r\n:500\r\n\$6\r\nfoobar\r\n"),
      Obj?["foo", 500, "foobar"])
    verifyErr(IOErr#) { read("-ERR\r\n") }
  }

  private Obj? read(Str s)
  {
    buf := Buf().print(s)
    return RespReader(buf.flip.in).read
  }
}