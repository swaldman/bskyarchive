package com.mchange.bskyarchive

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.Base64
import org.apache.commons.codec.binary.Base32

/**
  * See https://github.com/multiformats/cid
  */
object Cid:
  /**
   * Parse a CID from a multibase base32 string (e.g., "bafyrei...").
   */
  def fromMultibaseCidBase32(s: String): Cid =
    require(s.length > 0 && s.charAt(0) == 'b', s"Invalid multibase CID: must start with 'b', got: $s")

    // Remove the 'b' prefix
    val base32Str = s.substring(1)

    // Decode from base32
    val base32 = new Base32()
    val bytes = base32.decode(base32Str.toUpperCase)

    // Parse the CID from the decoded bytes
    val (cid, _) = readBinary(ByteArrayInputStream(bytes))
    cid
  /**
   * Parse a CID from an InputStream.
   * Returns the CID and the number of bytes consumed.
   */
  def readBinary(in: InputStream): (Cid, Int) =
    var bytesRead = 0

    // Read version
    val version = in.read()
    if version == -1 then
      throw new IllegalStateException("Unexpected EOF reading CID version")
    bytesRead += 1

    // Read codec (varint)
    val (codec, codecBytes) = Varint.read(in)
    bytesRead += codecBytes

    // Read hash algorithm (varint)
    val (hashAlg, hashAlgBytes) = Varint.read(in)
    bytesRead += hashAlgBytes

    // Read hash length (varint)
    val (hashLen, hashLenBytes) = Varint.read(in)
    bytesRead += hashLenBytes

    // Read hash digest
    val digest = readBytes(in, hashLen.toInt)
    bytesRead += hashLen.toInt

    (Cid(version, codec, hashAlg, digest), bytesRead)

  /**
   * Parse a CID from a byte array.
   * CBOR-embedded CIDs (tag 42) have a leading 0x00 byte (multibase identity prefix).
   */
  def readBinary(bytes: Array[Byte]): Cid =
    // Skip leading 0x00 if present (multibase identity prefix used in CBOR)
    val startIdx = if bytes.length > 0 && bytes(0) == 0 then 1 else 0
    val (cid, _) = readBinary(ByteArrayInputStream(bytes, startIdx, bytes.length - startIdx))
    cid
    
case class Cid( version: Int, codec: Long, hashAlgorithm: Long, hashDigest: Array[Byte] ):
  def hashDigestBase64: String = Base64.getEncoder.encodeToString(hashDigest)

  def hashDigestHex: String = hashDigest.map(b => f"${b & 0xFF}%02x").mkString

  /**
   * Convert CID to base32 string (e.g., "bafyrei..." format used by IPFS/Bluesky).
   *
   * See https://github.com/multiformats/multibase
   *     https://github.com/multiformats/cid
   */
  def toMultibaseCidBase32: String =
    val out = new ByteArrayOutputStream()

    // Write version
    out.write(version.toByte)

    // Write codec as varint
    Varint.write(out, codec)

    // Write multihash: hash algorithm + hash length + digest
    Varint.write(out, hashAlgorithm)
    Varint.write(out, hashDigest.length.toLong)
    out.write(hashDigest)

    // Encode as base32 lowercase
    val base32 = new Base32()
    val encoded = base32.encodeAsString(out.toByteArray).toLowerCase
      .replace("=", "") // Remove padding

    // Add multibase prefix 'b' for base32 lowercase
    "b" + encoded

  override def toString: String =
    s"CID(v$version, codec=$codec, hash=$hashAlgorithm, digest=${hashDigestHex})"

  // Override equals and hashCode since we have an Array[Byte]
  override def equals(obj: Any): Boolean = obj match
    case other: Cid =>
      version == other.version &&
      codec == other.codec &&
      hashAlgorithm == other.hashAlgorithm &&
      java.util.Arrays.equals(hashDigest, other.hashDigest)
    case _ => false

  override def hashCode(): Int =
    val prime = 31
    var result = 1
    result = prime * result + version
    result = prime * result + codec.toInt
    result = prime * result + hashAlgorithm.toInt
    result = prime * result + java.util.Arrays.hashCode(hashDigest)
    result


