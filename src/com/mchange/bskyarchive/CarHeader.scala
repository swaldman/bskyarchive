package com.mchange.bskyarchive

import java.io.*
import java.util.Base64

import com.upokecenter.cbor.CBORObject

object CarHeader:
  def read( is : InputStream ) : CarHeader =
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

case class CarHeader( version: Long, roots: Seq[Cid] )

