//
// Copyright (c) 2023, Novant LLC
// Licensed under the MIT License
//
// History:
//   8 Jan 2023  Andy Frank  Creation
//

using concurrent

*************************************************************************
** TestServer
*************************************************************************

internal class TestServer
{
  const Str host := "localhost"
  const Int port := 5555

  ** Start local test redis-server proc.
  Void start(File procDir)
  {
    this.proc = Process {
      it.command = ["redis-server", "--port", "${port}"]
      it.dir = procDir
      it.out = null
    }
    this.proc.run
    Actor.sleep(500ms)
  }

  ** Teardown test redis-server proc.
  Void stop()
  {
    if (proc != null) this.proc.kill.join
  }

  private Process? proc
}