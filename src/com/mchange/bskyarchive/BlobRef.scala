package com.mchange.bskyarchive

import java.util.Base64

import com.upokecenter.cbor.CBORObject

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
case class BlobRef( cid: Cid, size: Long, mimeType: String ):
  lazy val simpleFileName =
    val cidBase32 = cid.toMultibaseCidBase32
    // Construct URL (Bluesky format)
    mimeType match
      case m if m.startsWith("video/") =>
        // Videos use video.bsky.app with HLS playlist
        s"$cidBase32.m3u8"
      case m if m.contains("jpeg") || m.contains("jpg") =>
        s"$cidBase32.jpg"
      case m if m.contains("png") =>
        s"$cidBase32.png"
      case m if m.contains("webp") =>
        s"$cidBase32.webp"
      case _ =>
        s"$cidBase32.jpeg"
  def toBskyUrl( did : String ) =
    val cidBase32 = cid.toMultibaseCidBase32
    // Construct URL (Bluesky format)
    mimeType match
      case m if m.startsWith("video/") =>
        // Videos use video.bsky.app with HLS playlist
        s"https://video.bsky.app/watch/$did/$cidBase32/playlist.m3u8"
      case m if m.contains("jpeg") || m.contains("jpg") =>
        s"https://cdn.bsky.app/img/feed_fullsize/plain/$did/$cidBase32@jpeg"
      case m if m.contains("png") =>
        s"https://cdn.bsky.app/img/feed_fullsize/plain/$did/$cidBase32@png"
      case m if m.contains("webp") =>
        s"https://cdn.bsky.app/img/feed_fullsize/plain/$did/$cidBase32@webp"
      case _ =>
        s"https://cdn.bsky.app/img/feed_fullsize/plain/$did/$cidBase32@jpeg"
