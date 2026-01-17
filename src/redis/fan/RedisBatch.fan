//
// Copyright (c) 2026, Novant LLC
// Licensed under the MIT License
//
// History:
//   17 Jan 2026  Andy Frank  Creation
//

using concurrent
using inet

**************************************************************************
** RedisBatch
**************************************************************************

** RedisBatch builds a list of commands that can be
** run in batch using 'pipeline' or 'multi'.
class RedisBatch
{

//////////////////////////////////////////////////////////////////////////
// Identity
//////////////////////////////////////////////////////////////////////////

  ** Return number of commands in this batch.
  Int size() { cmds.size }

  ** Iterate the current commands in this batch.
  Void each(|Obj[] cmd| f) { cmds.each(f) }

//////////////////////////////////////////////////////////////////////////
// Base API
//////////////////////////////////////////////////////////////////////////

  ** Get the value for given key.
  This get(Str key)
  {
    cmds.add(["GET", key])
    return this
  }

  ** Set the given key to value, if 'val' is null this method deletes
  ** the given key (see `del`). If 'px' is non-null expire this key
  ** after the given timeout in milliseconds.
  This set(Str key, Obj? val, Duration? px := null)
  {
    // delete if key 'null'
    if (val == null) return del(key)

    // else set
    req := ["SET", key, val]
    if (px != null) req.add("PX").add(toMillis(px))
    cmds.add(req)
    return this
  }

  ** Set the given key to value only if key does not exist. Returns
  ** 'true' if set was succesfull, or false if set failed due to
  ** already existing key.  If 'px' is non-null expire this key after
  ** the given timeout in milliseconds.
  This setnx(Str key, Obj val, Duration? px := null)
  {
    req := ["SET", key, val, "NX"]
    if (px != null) req.add("PX").add(toMillis(px))
    cmds.add(req)
    return this
  }

  ** Delete the given key value.
  This del(Str key)
  {
    cmds.add(["DEL", key])
    return this
  }

//////////////////////////////////////////////////////////////////////////
// Expire
//////////////////////////////////////////////////////////////////////////

  ** Expire given key after given 'seconds' has elasped, where
  ** timeout must be in even second intervals.
  This expire(Str key, Duration seconds)
  {
    sec := toSec(seconds)
    cmds.add(["EXPIRE", key, sec])
    return this
  }

  ** Expire given key when the given 'timestamp' has been reached,
  ** where 'timestamp' has a resolution of whole seconds.
  This expireat(Str key, DateTime timestamp)
  {
    unix := timestamp.toJava / 1000
    cmds.add(["EXPIREAT", key, unix])
    return this
  }

  ** Expire given key after given 'ms' has elasped, where
  ** timeout must be in even millisecond intervals.
  This pexpire(Str key, Duration milliseconds)
  {
    ms := toMillis(milliseconds)
    cmds.add(["PEXPIRE", key, ms])
    return this
  }

//////////////////////////////////////////////////////////////////////////
// Incr
//////////////////////////////////////////////////////////////////////////

  ** Increments the number stored at key by one. If the key does
  ** not exist, it is set to 0 before performing the operation.
  ** If 'px' is non-null expire this key after the given timeout
  ** in milliseconds. Returns the value of the key after the increment.
  This incr(Str key)
  {
    cmds.add(["INCR", key])
    return this
  }

  ** Increments the number stored at key by 'delta'. If the key
  ** does not exist, it is set to 0 before performing the operation.
  ** If 'px' is non-null expire this key after the given timeout
  ** in milliseconds. Returns the value of the key after the increment.
  This incrby(Str key, Int delta)
  {
    cmds.add(["INCRBY", key, delta])
    return this
  }

  ** Increment the string representing a floating point number
  ** stored at 'key' by the specified 'delta'. If the key does not
  ** exist, it is set to 0 before performing the operation. If 'px'
  ** is non-null expire this key after the given timeout in
  ** milliseconds. Returns the value of the key after the increment.
  This incrbyfloat(Str key, Float delta)
  {
    cmds.add(["INCRBYFLOAT", key, delta])
    return this
  }

//////////////////////////////////////////////////////////////////////////
// Append
//////////////////////////////////////////////////////////////////////////

  /*
  ** If 'key' already exists and is a string, this command appends
  ** the value at the end of the string. If 'key' does not exist
  ** it is created and set as an empty string, so 'append' will be
  ** similar to `set` in this special case.
  This append(Str key, Obj val)
  {
    cmds.add(["APPEND", key, val])
    return this
  }
  */

//////////////////////////////////////////////////////////////////////////
// Sets
//////////////////////////////////////////////////////////////////////////

  /*
  ** Add the specified member to the set stored at key, or ignore
  ** if this member already exists in this set. If key does not exist,
  ** a new set is created before adding the specified members.
  Void sadd(Str key, Obj member)
  {
    invoke(["SADD", key, member])
  }

  ** Add the specified members to the set stored at key. Specified
  ** members that are already a member of this set are ignored. If
  ** key does not exist, a new set is created before adding the
  ** specified members.
  Void saddAll(Str key, Obj[] members)
  {
    invoke(Obj["SADD", key].addAll(members))
  }

  ** Remove the specified member from the set stored at key, or do
  ** nothing if member does not exist in this set.  If key does not
  ** exist, this method does nothing.
  Void srem(Str key, Obj member)
  {
    invoke(["SREM", key, member])
  }

  ** Remove the specified member from the set stored at key. Specified
  ** members that are not a member of this set are ignored. If key does
  ** not exist, this method does nothing.
  Void sremAll(Str key, Obj[] members)
  {
    invoke(Obj["SREM", key].addAll(members))
  }

  ** Returns the set cardinality (number of elements) of the set
  ** stored at key.
  Int scard(Str key)
  {
    invoke(["SCARD", key])
  }

  ** Returns 'true' if member is a member of the set stored at key.
  Bool sismember(Str key, Obj member)
  {
    invoke(["SISMEMBER", key, member]) == 1
  }

  ** Returns all the members of the set value stored at key.
  Str[] smembers(Str key)
  {
    invoke(["SMEMBERS", key])
  }
  */

  // `sadd`, `srem`, `scard`, `sismember`, `smembers`

//////////////////////////////////////////////////////////////////////////
// Hash
//////////////////////////////////////////////////////////////////////////

  /*
  ** Get the hash field for given key.
  Str? hget(Str key, Str field)
  {
    invoke(["HGET", key, field])
  }

  ** Get the hash field for given key.
  Str?[] hmget(Str key, Str[] fields)
  {
    invoke(["HMGET", key].addAll(fields))
  }

  ** Get all hash field values for given key.
  Str:Str hgetall(Str key)
  {
    map := Str:Str[:]
    List acc := invoke(["HGETALL", key])

    for (i:=0; i<acc.size; i+=2)
    {
      k := acc[i]
      v := acc[i+1]
      map[k] = v
    }

    return map
  }

  ** Set the hash field to the given value for key.
  Void hset(Str key, Str field, Obj val)
  {
    invoke(["HSET", key, field, val])
  }

  ** Set all hash values in 'vals' for given key.
  Void hmset(Str key, Str:Obj vals)
  {
    acc := Obj["HMSET", key]
    vals.each |v,k| { acc.add(k).add(v) }
    invoke(acc)
  }

  ** Delete given hash field for key.
  Void hdel(Str key, Str field)
  {
    invoke(["HDEL", key, field])
  }

  ** Convenience for 'hincrby(key, field 1)'
  Int hincr(Str key, Str field)
  {
    hincrby(key, field, 1)
  }

  ** Increments the number stored at 'field' in the hash stored at
  ** 'key' by given 'delta'. If the field does not exist, it is set
  ** to 0 before performing the operation.
  Int hincrby(Str key, Str field, Int delta)
  {
    invoke(["HINCRBY", key, field, delta])
  }

  ** Increment the string representing a floating point number stored
  ** at 'field' in the hash stored at 'key' by the specified 'delta'.
  ** If the key does not exist, it is set to 0 before performing the
  ** operation.
  Float hincrbyfloat(Str key, Str field, Float delta)
  {
    Str res := invoke(["HINCRBYFLOAT", key, field, delta])
    return res.toFloat
  }
  */

//////////////////////////////////////////////////////////////////////////
// Support
//////////////////////////////////////////////////////////////////////////

  ** Convert duration to even millis or throw if < 1ms
  private Int toMillis(Duration d)
  {
    ms := d.toMillis
    if (ms < 1) throw ArgErr("Non-zero timeout in milliseconds required")
    return ms
  }

  ** Convert duration to even millis or throw if < 1ms
  private Int toSec(Duration d)
  {
    sec := d.toSec
    if (sec < 1) throw ArgErr("Non-zero timeout in seconds required")
    return sec
  }

//////////////////////////////////////////////////////////////////////////
// Fields
//////////////////////////////////////////////////////////////////////////

  internal Obj[] cmds := [,]
}