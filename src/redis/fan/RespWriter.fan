//
// Copyright (c) 2021, Novant LLC
// Licensed under the MIT License
//
// History:
//   11 Apr 2021  Andy Frank  Creation
//

**
** RESP (REdis Serialization Protocol) writer.
**
internal class RespWriter
{
  ** Create a new RespWriter for given OutStream.
  new make(OutStream out)
  {
    this.out = out
  }

  ** Write value to OutStream.
  This write(Obj? val)
  {
    if (val == null) return writeNull
    if (val is List) return writeList(val)
    return writeStr(val.toStr)
  }

  ** Flush contents.
  This flush()
  {
    out.flush
    return this
  }

  ** Write a null val.
  private This writeNull()
  {
    out.writeChars("\$-1")
    writeCRLF
    return this
  }

  ** Write a bulk Str.
  private This writeStr(Str val)
  {
    // TODO FIXIT: size is incorrect if we need to encoding
    // non-ascii chars; we could use Str.toBuf here to fix
    // but can we avoid the double loop?

    out.writeChar('\$')
    out.writeChars(val.size.toStr)
    writeCRLF
    out.writeChars(val)
    writeCRLF
    return this
  }

  ** Write a list.
  private This writeList(Obj?[] val)
  {
    out.writeChar('*')
    out.writeChars(val.size.toStr)
    writeCRLF
    val.each |v| { write(v) }
    return this
  }

  ** Convenience to write '\r\n'.
  private This writeCRLF()
  {
    out.writeChar('\r').writeChar('\n')
    return this
  }

  private OutStream out
}