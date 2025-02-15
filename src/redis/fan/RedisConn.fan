//
// Copyright (c) 2023, Novant LLC
// Licensed under the MIT License
//
// History:
//   8 Jan 2023  Andy Frank  Creation
//

using concurrent
using inet

**
** Redis client connection.
**
internal class RedisConn
{
  ** Open a new Redis API client connection to given host.
  static new open(Str host, Int port)
  {
    s := TcpSocket(SocketConfig {
      it.connectTimeout = 10sec
      it.receiveTimeout = 10sec
    })
    s.connect(IpAddr(host), port)
    return RedisConn(s)
  }

  ** Private ctor.
  private new make(TcpSocket s)
  {
    this.socket = s
  }

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

  ** Return 'true if connection is closed.
  Bool isClosed() { socket.isClosed }

  ** Close this connection.
  Void close() { socket.close }

  ** Flag used by 'RedisConnPool' to "lock"
  internal AtomicBool inuse := AtomicBool(false)

  private TcpSocket socket
}