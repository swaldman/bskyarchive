package com.mchange.bskyarchive

import java.time.Instant
import scala.util.{Try, Success, Failure}

/**
 * Timestamp Identifier (TID) as defined in AT Protocol.
 *
 * A TID is a 13-character base32-sortable string encoding a 64-bit integer.
 * The integer contains:
 * - 1 bit: always 0
 * - 53 bits: microseconds since UNIX epoch
 * - 10 bits: random clock identifier (for collision resistance)
 *
 * See https://atproto.com/specs/tid
 */
object Tid:
  // Base32-sortable character set used for TIDs
  private val base32SortableChars = "234567abcdefghijklmnopqrstuvwxyz"
  private val charToValue: Map[Char, Int] = base32SortableChars.zipWithIndex.toMap

  // Valid first characters (subset of full character set)
  private val validFirstChars = "234567abcdefghij".toSet

  // Regex pattern for validation
  private val tidPattern = "^[234567abcdefghij][234567abcdefghijklmnopqrstuvwxyz]{12}$".r

  /**
   * Parse a TID string.
   * Returns Some(Tid) if valid, None if invalid.
   */
  def parse(s: String): Option[Tid] =
    if isValid(s) then
      Try {
        val value = decode(s)
        Tid(s, value)
      }.toOption
    else
      None

  /**
   * Validate a TID string format.
   */
  def isValid(s: String): Boolean =
    s.length == 13 && tidPattern.matches(s)

  /**
   * Decode a base32-sortable TID string to its 64-bit integer value.
   */
  private def decode(s: String): Long =
    var result = 0L
    var i = 0
    while i < s.length do
      val char = s.charAt(i)
      val value = charToValue.getOrElse(char,
        throw new IllegalArgumentException(s"Invalid character in TID: $char"))
      result = (result << 5) | value.toLong
      i += 1
    result

  /**
   * Encode a 64-bit integer to a base32-sortable TID string.
   */
  private def encode(value: Long): String =
    val chars = new Array[Char](13)
    var remaining = value
    var i = 12
    while i >= 0 do
      val idx = (remaining & 0x1FL).toInt
      chars(i) = base32SortableChars.charAt(idx)
      remaining = remaining >>> 5
      i -= 1
    new String(chars)

  /**
   * Create a TID from a 64-bit integer value.
   */
  def fromLong(value: Long): Tid =
    val str = encode(value)
    Tid(str, value)

  /**
   * Create a TID from an Instant and optional clock identifier.
   */
  def fromInstant(instant: Instant, clockId: Int = scala.util.Random.nextInt(1024)): Tid =
    require(clockId >= 0 && clockId < 1024, "Clock identifier must be 0-1023")

    val micros = instant.getEpochSecond * 1000000L + instant.getNano / 1000
    val value = (micros << 10) | clockId.toLong
    fromLong(value)

  /**
   * Generate a new TID with the current timestamp.
   */
  def generate(): Tid =
    fromInstant(Instant.now())

case class Tid(str: String, value: Long):
  require(Tid.isValid(str), s"Invalid TID format: $str")

  /**
   * Extract the timestamp (microseconds since UNIX epoch) from the TID.
   */
  def timestampMicros: Long = value >>> 10

  /**
   * Extract the clock identifier from the TID.
   */
  def clockId: Int = (value & 0x3FFL).toInt

  /**
   * Get the timestamp as an Instant.
   */
  def toInstant: Instant =
    val micros = timestampMicros
    val seconds = micros / 1000000L
    val nanos = ((micros % 1000000L) * 1000L).toInt
    Instant.ofEpochSecond(seconds, nanos)

  override def toString: String = str

  override def equals(obj: Any): Boolean = obj match
    case other: Tid => str == other.str
    case _ => false

  override def hashCode(): Int = str.hashCode
