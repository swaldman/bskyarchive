package com.mchange.bskyarchive

import java.io.*

import java.time.Instant
import java.time.format.DateTimeFormatter
import DateTimeFormatter.{ISO_INSTANT,ISO_OFFSET_DATE_TIME}

import scala.util.Try

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

// see https://atproto.com/specs/lexicon#datetime
def parseDateTime( dateTime : String ) : Instant =
  def fromIsoInstant =
    Try( Instant.from( ISO_INSTANT.parse(dateTime) ) )
  def fromIsoOffsetDateTime =
    Try( Instant.from( ISO_OFFSET_DATE_TIME.parse(dateTime) ) )
  fromIsoInstant.orElse(fromIsoOffsetDateTime).get

