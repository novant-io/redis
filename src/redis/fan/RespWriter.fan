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
    // TODO: can we optimize binary-safe write to avoid toBuf?
    buf := val.toBuf
    out.writeChar('\$')
    out.writeChars(buf.size.toStr)
    writeCRLF
    out.writeBuf(buf)
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