package com.mchange.bskyarchive

import java.io.*

import com.upokecenter.cbor.CBORObject

import com.mchange.bskyarchive.exception.UnexpectedRecordType

object CarBlock:
  enum Type:
    case Commit, Node, Record

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

      Some(CarBlock(cid, data))

  def apply( cid : Cid, data : Array[Byte] ) : CarBlock =
    val cbor = CBORObject.DecodeFromBytes(data)
    val `type` =
      if cbor.ContainsKey("e") then Type.Node
      else if cbor.ContainsKey("did") then Type.Commit
      else if cbor.ContainsKey("$type") then Type.Record
      else
        throw new UnexpectedRecordType("Cannot recognize type of block: " +  cbor.ToJSONString())
    CarBlock( cid, cbor, `type` )
case class CarBlock private ( cid: Cid, cbor : CBORObject, `type` : CarBlock.Type ):
  lazy val json  : String = cbor.ToJSONString()
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
