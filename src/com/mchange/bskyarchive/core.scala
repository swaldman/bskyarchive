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
  override def toString: String = s"PostReference($atUri, ${cid.toMultibaseCidBase32})"

object PostReference:
  def fromCBOR(cbor: CBORObject): PostReference =
    val uri = cbor.get("uri").AsString()
    val cidBytes = cbor.get("cid").GetByteString()
    val cid = Cid.readBinary(cidBytes)
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
