package com.mchange.bskyarchive

import java.io.*

object Varint:
  /**
   * Read a varint from an InputStream.
   * Returns the decoded value and the number of bytes read.
   */
  def read(in : InputStream): (Long, Int) =
    var value = 0L
    var shift = 0
    var bytesRead = 0
    var continue = true

    while continue do
      val b = in.read()
      if b == -1 then
        throw new IllegalStateException("Unexpected EOF while reading varint")

      bytesRead += 1
      value |= ((b & 0x7F).toLong << shift)

      if (b & 0x80) == 0 then
        continue = false
      else
        shift += 7

    (value, bytesRead)

  def write(out : OutputStream, value : Long): Unit =
    var v = value
    while v >= 0x80 do
      out.write(((v & 0x7F) | 0x80).toByte)
      v >>>= 7
    out.write(v.toByte)


