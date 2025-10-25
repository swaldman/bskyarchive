package com.mchange.bskyarchive

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import com.upokecenter.cbor.CBORObject

abstract class AbstractBskyRepoReader( is : InputStream  ) extends AutoCloseable:
  private var isClosed = false

  val header : CarHeader = CarHeader.read(is)

  // this is better than the LazyList version, because we don't want to memoize
  def blocks : IterableOnce[CarBlock]

  def close() : Unit =
    this.synchronized:
      if !isClosed then
        isClosed = true
        is.close()

