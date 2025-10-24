package com.mchange.bskyarchive

import java.io.{InputStream,OutputStream}
import java.util.Base64

import com.upokecenter.cbor.CBORObject

import com.mchange.bskyarchive.exception.UnexpectedRecordType

case class CarHeader( version: Long, roots: Seq[Cid] )

/**
 * Represents a blob reference in AT Protocol.
 * Blobs are binary data (images, videos, etc.) referenced by CID.
 */
object BlobRef:
  def fromCBORMap(cbor: CBORObject): Option[BlobRef] =
    if !cbor.ContainsKey("$type") then
      return None

    val typeStr = try cbor.get("$type").AsString() catch case _ => return None

    if typeStr != "blob" then
      None
    else
      try
        // The ref field contains the CID
        // It might be stored as CBOR tag 42 (CID link) or as a base64 string
        val refObj = cbor.get("ref")

        val cid = if refObj.HasTag(42) then
          // It's a CBOR tag 42 (CID link) - extract the bytes
          val cidBytes = refObj.GetByteString()
          Cid.readBinary(cidBytes)
        else if refObj.getType() == com.upokecenter.cbor.CBORType.TextString then
          // It's a base64 string - decode it
          val base64Str = refObj.AsString()
          val cidBytes = Base64.getDecoder.decode(base64Str)
          Cid.readBinary(cidBytes)
        else
          // It's raw bytes
          val cidBytes = refObj.GetByteString()
          Cid.readBinary(cidBytes)

        val size = cbor.get("size").AsInt64Value()
        val mimeType = cbor.get("mimeType").AsString()

        Some(BlobRef(cid, size, mimeType))
      catch
        case e: Exception =>
          println(s"  Warning: Failed to parse blob ref: ${e.getMessage}")
          None
case class BlobRef( cid: Cid, size: Long, mimeType: String )

object CarBlock:
  enum Type:
    case Commit, Node, Record
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


