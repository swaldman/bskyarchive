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

import com.upokecenter.cbor.CBORObject

/**
 * A strong reference to another record in AT Protocol.
 * Combines an AT URI with a CID for content-addressed verification.
 *
 * See https://atproto.com/specs/lexicon
 */
case class PostReference( cid : Cid, atUri : String ):
  override def toString: String = s"PostReference(cid=${cid.toMultibaseCidBase32}, atUri=${atUri})"

object PostReference:
  def fromCBOR(cbor: CBORObject): PostReference =
    val uri = cbor.get("uri").AsString()
    val cidObj = cbor.get("cid")

    // Handle various CID encoding formats
    val cid = cidObj.getType() match
      case com.upokecenter.cbor.CBORType.ByteString =>
        // Raw byte string (may or may not have tag 42)
        Cid.readBinary(cidObj.GetByteString())
      case com.upokecenter.cbor.CBORType.TextString =>
        // Multibase base32 encoded CID string (e.g., "bafyrei...")
        Cid.fromMultibaseCidBase32(cidObj.AsString())
      case _ =>
        throw new IllegalStateException(
          s"Unexpected CBOR type for CID in PostReference: ${cidObj.getType()}, hasTag42=${cidObj.HasTag(42)}, value=${cidObj.ToJSONString()}"
        )

    PostReference(cid, uri)

/**
 * Reply reference in a Bluesky post.
 * Contains references to both the thread root and immediate parent post.
 */
case class Reply( root : PostReference, parent : PostReference ):
  override def toString: String = s"Reply(root=${root.atUri}, parent=${parent.atUri})"

object Reply:
  def fromCBOR(cbor: CBORObject): Reply =
    val root = PostReference.fromCBOR(cbor.get("root"))
    val parent = PostReference.fromCBOR(cbor.get("parent"))
    Reply(root, parent)

case class AspectRatio( width : Int, height : Int )

object AspectRatio:
  def fromCBOR(cbor: CBORObject): AspectRatio =
    val width = cbor.get("width").AsInt32Value()
    val height = cbor.get("height").AsInt32Value()
    AspectRatio(width, height)

case class ImageRef( image : BlobRef, alt : Option[String], aspectRatio : Option[AspectRatio] )

object ImageRef:
  def fromCBOR(cbor: CBORObject): ImageRef =
    val image = BlobRef.fromCBORMap(cbor.get("image")).get
    val alt = 
      val raw = cbor.get("alt").AsString()
      if raw.trim.isEmpty then None else Some( raw )
    val aspectRatio = if cbor.ContainsKey("aspectRatio") then
      Some(AspectRatio.fromCBOR(cbor.get("aspectRatio")))
    else
      None
    ImageRef(image, alt, aspectRatio)

