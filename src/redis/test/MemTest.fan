//
// Copyright (c) 2024, Novant LLC
// All Rights Reserved
//
// History:
//   10 Jan 2024  Andy Frank  Creation
//

using concurrent

*************************************************************************
** MemTest
*************************************************************************

@NoDoc class MemTest : AbstractRedisTest
{
  Void testMemStats()
  {
    startServer
    r := makeClient
    m := r.memStats

    // spot check some values we expect to exist
    verifyTrue(m["peak.allocated"] is Int)
    verifyTrue(m["replication.backlog"]is Int)
    verifyTrue(m["keys.count"] is Int)
  }
}