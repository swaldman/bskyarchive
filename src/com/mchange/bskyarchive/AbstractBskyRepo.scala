package com.mchange.bskyarchive

import java.io.{ByteArrayInputStream, InputStream, SequenceInputStream}
import com.upokecenter.cbor.CBORObject

abstract class AbstractBskyRepo( is : InputStream  ) extends AutoCloseable:
  private var isClosed = false

  val header : CarHeader = parseHeader

  // this is better than the LazyList version, because we don't want to memoize
  def blocks : IterableOnce[CarBlock]

  def close() : Unit =
    this.synchronized:
      if !isClosed then
        isClosed = true
        is.close()

  /*
   * Parse a single block from the CAR file.
   * Returns None if EOF is reached.
   */
  protected def parseBlock: Option[CarBlock] =
    // Try to read block length
    val firstByte = is.read()
    if firstByte == -1 then
      this.close()
      None
    else  
      // Put the first byte back by creating a sequence
      val varintStream = new SequenceInputStream(
        new ByteArrayInputStream(Array(firstByte.toByte)),
        is
      )

      val (blockLen, _) = Varint.read(varintStream)

      // Read the entire block
      val blockBytes = readBytes(is, blockLen.toInt)

      // Parse CID from the beginning of the block
      val blockStream = ByteArrayInputStream(blockBytes)
      val (cid, cidLen) = Cid.readBinary(blockStream)

      // Remaining bytes are the data
      val dataLen = blockLen.toInt - cidLen
      val data = blockBytes.slice(cidLen, blockBytes.length)

      Some(CarBlock(cid, data))

  protected def parseHeader: CarHeader =
    // Read header length (varint)
    val (headerLen, _) = Varint.read(is)

    // Read header bytes
    val headerBytes = readBytes(is, headerLen.toInt)

    // Decode CBOR
    val cbor = CBORObject.DecodeFromBytes(headerBytes)

    // Extract version
    val version = cbor.get("version").AsInt64Value()

    // Extract roots (array of CIDs)
    val rootsArray = cbor.get("roots")
    val roots = (0 until rootsArray.size()).map { i =>
      val rootBytes = rootsArray.get(i).GetByteString()
      Cid.readBinary(rootBytes)
    }

    CarHeader(version, roots)
