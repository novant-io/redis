//
// Copyright (c) 2021, Novant LLC
// Licensed under the MIT License
//
// History:
//   11 Apr 2021  Andy Frank  Creation
//

**
** RESP (REdis Serialization Protocol) reader.
**
internal class RespReader
{
  ** Construct a new reader.
  new make(InStream in) { this.in = in }

  ** Read value from RESP stream.
  Obj? read()
  {
    op := in.read
    switch (op)
    {
      case ':':  return readInt
      case '+':  return readSimpleStr
      case '\$': return readBulkStr
      case '*':  return readList
      case '-':  throw IOErr(readSimpleStr)
      default:   throw IOErr("Unexpected char '${op.toChar}'")
    }
  }

  ** Read integer value.
  private Int readInt() { readVal.toInt }

  ** Read simple string value.
  private Str readSimpleStr() { readVal }

  ** Read bulk string value.
  private Str? readBulkStr()
  {
    len := readVal.toInt
    if (len == -1) return null
    val := in.readChars(len)
    in.read
    in.read
    return val
  }

  ** Read a list value.
  private Obj?[] readList()
  {
    acc := [,]
    len := readVal.toInt
    len.times { acc.add(read) }
    return acc
  }

  ** Read the next value up to '\r\n'. If 'eat'
  ** is true then consume the trailing '\r\n'.
  private Str readVal(Bool eat := true)
  {
    buf := StrBuf()
    while (in.peek != '\r')
    {
      buf.addChar(in.readChar)
    }
    if (eat) { in.read; in.read } // eat \r\n
    return buf.toStr
  }

  private InStream in
}