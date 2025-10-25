package com.mchange.bskyarchive

import java.io.*
import java.time.Instant

import com.upokecenter.cbor.CBORObject

import com.mchange.bskyarchive.exception.UnexpectedRecordType

object CarBlock:
  case class Commit(cid  : Cid, cbor : CBORObject) extends CarBlock:
    lazy val did = cbor.get("did").AsString()
    lazy val rev = cbor.get("rev").AsString().toInt
  case class Node(cid  : Cid, cbor : CBORObject) extends CarBlock
  case class Record(cid  : Cid, cbor : CBORObject) extends CarBlock:
    lazy val `type` = cbor.get("$type").AsString()
    lazy val createdAt : Instant = parseDateTime( cbor.get("createdAt").AsString() )
    lazy val facets = Facet.allFromRecord(cbor)
    lazy val text : Option[String] =
      if cbor.ContainsKey("text") then
        Some( cbor.get("text").AsString() )
      else
        None
    lazy val blobRefs : Seq[BlobRef] =
      val buffer = scala.collection.mutable.ArrayBuffer[BlobRef]()

      def scan(obj: CBORObject): Unit =
        if obj.getType() == com.upokecenter.cbor.CBORType.Map then
          // Check if this object is a blob
          BlobRef.fromCBORMap(obj).foreach(buffer += _)

          // Recursively scan all values in the map
          val keys = obj.getKeys()
          val keyIter = keys.iterator()
          while keyIter.hasNext() do
            val key = keyIter.next()
            scan(obj.get(key))

        else if obj.getType() == com.upokecenter.cbor.CBORType.Array then
          // Recursively scan array elements
          var i = 0
          while i < obj.size() do
            scan(obj.get(i))
            i += 1

      scan(cbor)
      buffer.toSeq

  /*
   * Parse a single block from the CAR file.
   * Returns None if EOF is reached.
   */
  def read( is : InputStream ) : Option[CarBlock] =
    // Try to read block length
    val firstByte = is.read()
    if firstByte == -1 then
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

      Some(this.apply(cid, data))

  def apply( cid : Cid, data : Array[Byte] ) : CarBlock =
    val cbor = CBORObject.DecodeFromBytes(data)
    if cbor.ContainsKey("e") then Node( cid, cbor )
    else if cbor.ContainsKey("did") then Commit( cid, cbor )
    else if cbor.ContainsKey("$type") then Record( cid, cbor )
    else
      throw new UnexpectedRecordType("Cannot recognize type of block: " +  cbor.ToJSONString())
trait CarBlock:
  def cid  : Cid
  def cbor : CBORObject
  
  lazy val json  : String = cbor.ToJSONString()
