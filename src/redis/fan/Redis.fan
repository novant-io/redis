//
// Copyright (c) 2021, Novant LLC
// Licensed under the MIT License
//
// History:
//   11 Apr 2021  Andy Frank  Creation
//

using inet

**
** Redis client.
**
class Redis
{
  ** Open a new Redis API client connection to given host.
  static new open(Str host, Int port := 6379)
  {
    s := TcpSocket()
    s.options.connectTimeout = 10sec
    s.options.receiveTimeout = 10sec
    s.connect(IpAddr(host), port)
    return Redis(s)
  }

  ** Private ctor.
  private new make(TcpSocket s)
  {
    this.socket = s
  }

  ** Convenience for 'invoke(["GET", key])'.
  Str? get(Str key) { invoke(["GET", key]) }

  ** Convenience for 'invoke(["SET", key, val])'.
  Void set(Str key, Obj val) { invoke(["SET", key, val]) }

  ** Convenience for 'invoke(["DEL", key])'.
  Void del(Str key) { invoke(["DEL", key]) }

  ** Invoke the given command and return response.
  Obj? invoke(Obj[] args)
  {
    // sanity check
    if (socket.isClosed) throw IOErr("Connection closed")

    // write req
    RespWriter(socket.out).write(args).flush

    // read resp
    return RespReader(socket.in).read
  }

  ** Pipeline multiple `invoke` requests and return batched results.
  Obj?[] pipeline(Obj[] invokes)
  {
    // batch writes
    w := RespWriter(socket.out)
    invokes.each |v| { w.write(v) }
    w.flush

    // read results
    r := RespReader(socket.in)
    acc := Obj?[,]
    invokes.size.times { acc.add(r.read) }
    return acc
  }

  ** Close this connection.
  Void close() { socket.close }

  private TcpSocket socket
}