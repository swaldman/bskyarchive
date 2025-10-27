package com.mchange.bskyarchive

import com.upokecenter.cbor.CBORObject

import com.mchange.bskyarchive.exception.UnparsableEmbed


/**
 * Base trait for all embed types in Bluesky posts.
 */
trait Embed

object Embed:
  /**
   * Image embed (app.bsky.embed.images)
   */
  case class Images( images : Seq[ImageRef] ) extends Embed

  /**
   * External link embed (app.bsky.embed.external)
   */
  object External:
    def fromCBOR(cbor: CBORObject): External =
      val uri = cbor.get("uri").AsString()
      val title = cbor.get("title").AsString()
      val description = 
        val raw = cbor.get("description").AsString()
        if raw.trim.isEmpty then None else Some(raw)
      val thumb = if cbor.ContainsKey("thumb") then
        BlobRef.fromCBORMap(cbor.get("thumb"))
      else
        None
      External(uri, title, description, thumb)
  case class External( uri : String, title : String, description : Option[String], thumb : Option[BlobRef] ) extends Embed

  /**
   * Record embed / quote post (app.bsky.embed.record)
   */
  case class Record( record : PostReference ) extends Embed

  /**
   * Record with media embed (app.bsky.embed.recordWithMedia)
   */
  case class RecordWithMedia( record : PostReference, media : Embed ) extends Embed

  /**
   * Video embed (app.bsky.embed.video)
   */
  case class Video( video : BlobRef, alt : Option[String], aspectRatio : Option[AspectRatio] ) extends Embed

  /**
   * Parse all embeds from a record's CBOR.
   */
  def forRecord(cbor: CBORObject): Option[Embed] =
    if !cbor.ContainsKey("embed") then
      None
    else
      val embedObj = cbor.get("embed")
      val embedType = embedObj.get("$type").AsString()

      val embedOpt: Option[Embed] = embedType match
        case "app.bsky.embed.images" =>
          val imagesArray = embedObj.get("images")
          val images = (0 until imagesArray.size()).map { i =>
            ImageRef.fromCBOR(imagesArray.get(i))
          }
          Some(Images(images.toSeq))

        case "app.bsky.embed.external" =>
          val externalObj = embedObj.get("external")
          Some( External.fromCBOR(externalObj) )

        case "app.bsky.embed.record" =>
          val recordObj = embedObj.get("record")
          val record = PostReference.fromCBOR(recordObj)
          Some(Record(record))

        case "app.bsky.embed.recordWithMedia" =>
          val recordObj = embedObj.get("record").get("record")
          val record = PostReference.fromCBOR(recordObj)
          val mediaObj = embedObj.get("media")
          val mediaType = mediaObj.get("$type").AsString()

          val mediaOpt: Option[Embed] = mediaType match
            case "app.bsky.embed.images" =>
              val imagesArray = mediaObj.get("images")
              val images = (0 until imagesArray.size()).map { i =>
                ImageRef.fromCBOR(imagesArray.get(i))
              }
              Some(Images(images.toSeq))
            case "app.bsky.embed.external" =>
              val externalObj = mediaObj.get("external")
              Some( External.fromCBOR(externalObj) )
            case "app.bsky.embed.video" =>
              val videoBlob = BlobRef.fromCBORMap(mediaObj.get("video")).get
              val alt = if mediaObj.ContainsKey("alt") then Some(mediaObj.get("alt").AsString()) else None
              val aspectRatio = if mediaObj.ContainsKey("aspectRatio") then
                Some(AspectRatio.fromCBOR(mediaObj.get("aspectRatio")))
              else
                None
              Some(Video(videoBlob, alt, aspectRatio))
            case other =>
              throw new UnparsableEmbed(s"Unknown media type '${other}' while parsing an Embed.RecordWithMedia.")

          mediaOpt.map(media => RecordWithMedia(record, media))

        case "app.bsky.embed.video" =>
          val videoBlob = BlobRef.fromCBORMap(embedObj.get("video")).get
          val alt = if embedObj.ContainsKey("alt") then Some(embedObj.get("alt").AsString()) else None
          val aspectRatio = if embedObj.ContainsKey("aspectRatio") then
            Some(AspectRatio.fromCBOR(embedObj.get("aspectRatio")))
          else
            None
          Some(Video(videoBlob, alt, aspectRatio))

        case other =>
          throw new UnparsableEmbed(s"Unknown embed type '${other}' while parsing an Embed.")

      embedOpt
