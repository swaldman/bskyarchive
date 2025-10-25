package com.mchange.bskyarchive

import com.upokecenter.cbor.CBORObject

object Facet:
  def allFromRecord( cbor : CBORObject ) : Set[Facet] =
    if !cbor.ContainsKey("facets") then
      Set.empty
    else
      val facetsArray = cbor.get("facets")
      val builder = Set.newBuilder[Facet]

      var i = 0
      while i < facetsArray.size() do
        val facetObj = facetsArray.get(i)
        val index = facetObj.get("index")
        val byteStart = index.get("byteStart").AsInt32Value()
        val byteEnd = index.get("byteEnd").AsInt32Value()

        val features = facetObj.get("features")
        var j = 0
        while j < features.size() do
          val feature = features.get(j)
          val featureType = feature.get("$type").AsString()

          featureType match
            case "app.bsky.richtext.facet#link" =>
              val uri = feature.get("uri").AsString()
              builder += Link(uri, byteStart, byteEnd, featureType)
            case "app.bsky.richtext.facet#tag" =>
              val tag = feature.get("tag").AsString()
              builder += Tag(tag, byteStart, byteEnd, featureType)
            case "app.bsky.richtext.facet#mention" =>
              val did = feature.get("did").AsString()
              builder += Mention(did, byteStart, byteEnd, featureType)
            case _ =>
              // Unknown facet type, skip
              println( s"Skipping unknown facet type: $featureType" )
              ()

          j += 1
        i += 1

      builder.result()

  case class Link( uri : String, byteStart : Int, byteEnd : Int, `type` : String ) extends Facet
  case class Tag( tag : String, byteStart : Int, byteEnd : Int, `type` : String ) extends Facet
  case class Mention( did : String, byteStart : Int, byteEnd : Int, `type` : String ) extends Facet
trait Facet:
  def byteStart : Int
  def byteEnd   : Int
  def `type`    : String
