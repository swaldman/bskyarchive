package com.mchange.bskyarchive

import java.io.*

import com.upokecenter.cbor.CBORObject

/**
 * Read exactly n bytes from an InputStream.
 */
def readBytes(in: InputStream, n: Int): Array[Byte] =
  val bytes = new Array[Byte](n)
  var offset = 0
  while offset < n do
    val read = in.read(bytes, offset, n - offset)
    if read == -1 then
      throw new IllegalStateException(s"Unexpected EOF: expected $n bytes, got $offset")
    offset += read
  bytes
