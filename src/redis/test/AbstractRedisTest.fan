//
// Copyright (c) 2021, Novant LLC
// Licensed under the MIT License
//
// History:
//   11 Apr 2021  Andy Frank  Creation
//

using concurrent

*************************************************************************
** AbstractRedisTest
*************************************************************************

@NoDoc abstract class AbstractRedisTest : Test
{
  private TestServer? server

  ** Start local test redis-server proc.
  protected Void startServer()
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

  protected RedisClient makeClient()
  {
    RedisClient(server.host, server.port)
  }
}