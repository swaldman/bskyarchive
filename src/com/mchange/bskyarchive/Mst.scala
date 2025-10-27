package com.mchange.bskyarchive

import com.upokecenter.cbor.CBORObject

/**
 * Merkle Search Tree (MST) data structures for AT Protocol repositories.
 *
 * The MST stores a mapping from record keys (rkeys/TIDs) to record CIDs,
 * using a content-addressed tree structure with prefix compression.
 *
 * See https://atproto.com/specs/repository
 */

/**
 * Helper to extract CID from CBOR object, handling both tagged and untagged formats.
 */
private def extractCid(cborObj: CBORObject): Cid =
  // CIDs can be encoded in multiple ways:
  // 1. CBOR tag 42 with byte string
  // 2. Raw byte string
  // 3. Text string (base32/base58 encoded)

  val objType = cborObj.getType()

  if objType == com.upokecenter.cbor.CBORType.ByteString then
    // Raw byte string (may or may not have tag 42)
    val cidBytes = cborObj.GetByteString()
    Cid.readBinary(cidBytes)
  else if objType == com.upokecenter.cbor.CBORType.TextString then
    // Text string - this shouldn't happen for CIDs in MST, but handle it gracefully
    throw new IllegalStateException(s"Unexpected text string CID in MST: ${cborObj.AsString()}")
  else
    // Unknown type - provide helpful error
    throw new IllegalStateException(
      s"Unexpected CBOR type for CID: ${objType}, hasTag42=${cborObj.HasTag(42)}, value=${cborObj.ToJSONString()}"
    )

/**
 * A single entry in an MST node.
 *
 * @param prefixLen Number of bytes shared with the previous entry's key (0 for first entry)
 * @param keySuffix Remainder of the key after removing the shared prefix
 * @param value CID pointing to the record
 * @param tree Optional CID pointing to a subtree (keys between this and next entry)
 */
case class MstEntry(
  prefixLen: Int,
  keySuffix: Array[Byte],
  value: Cid,
  tree: Option[Cid]
):
  /**
   * Reconstruct the full key for this entry.
   * Requires the previous entry's full key (or empty array for first entry).
   */
  def reconstructKey(prevKey: Array[Byte]): Array[Byte] =
    val prefix = prevKey.take(prefixLen)
    prefix ++ keySuffix

  override def toString: String =
    val keySuffixStr = new String(keySuffix, "UTF-8")
    val treeStr = tree.map(c => s", tree=${c.toMultibaseCidBase32}").getOrElse("")
    s"MstEntry(p=$prefixLen, k=$keySuffixStr, v=${value.toMultibaseCidBase32}$treeStr)"

object MstEntry:
  /**
   * Parse an MST entry from CBOR.
   */
  def fromCBOR(cbor: CBORObject): MstEntry =
    val p = cbor.get("p").AsInt32Value()
    val k = cbor.get("k").GetByteString()

    // Value is a CID (may be CBOR tag 42 or raw bytes)
    val vCid = extractCid(cbor.get("v"))

    // Tree is optional and may be null
    val t = if cbor.ContainsKey("t") then
      val tObj = cbor.get("t")
      if tObj.isNull() then
        None
      else
        Some(extractCid(tObj))
    else
      None

    MstEntry(p, k, vCid, t)

/**
 * Parsed MST node with reconstructed full keys.
 *
 * @param leftLink Optional CID to left subtree
 * @param entries Array of MST entries with their reconstructed full keys
 */
case class MstNode(
  leftLink: Option[Cid],
  entries: Array[(String, MstEntry)] // (reconstructed key string, entry)
):
  /**
   * Find the entry that points to a given record CID.
   * Returns the TID (record key) if found.
   */
  def findKeyByCid(recordCid: Cid): Option[String] =
    entries.find { case (_, entry) => entry.value == recordCid }
      .map { case (key, _) => key }

  /**
   * Get all record CIDs and their corresponding TIDs in this node.
   */
  def allRecords: Seq[(String, Cid)] =
    entries.map { case (tid, entry) => (tid, entry.value) }.toSeq

  /**
   * Get all subtree CIDs (for traversing the full MST).
   */
  def allSubtreeCids: Seq[Cid] =
    val trees = entries.flatMap(_._2.tree).toSeq
    leftLink match
      case Some(left) => left +: trees
      case None => trees

  override def toString: String =
    val leftStr = leftLink.map(c => s"left=${c.toMultibaseCidBase32}, ").getOrElse("")
    val entriesStr = entries.map { case (key, entry) => s"  $key -> ${entry.value.toMultibaseCidBase32}" }.mkString("\n")
    s"MstNode($leftStr${entries.length} entries):\n$entriesStr"

object MstNode:
  /**
   * Parse an MST node from CBOR and reconstruct full keys.
   */
  def fromCBOR(cbor: CBORObject): MstNode =
    // Parse optional left link
    val leftLink = if cbor.ContainsKey("l") then
      val lObj = cbor.get("l")
      // Check if it's null (CBOR SimpleValue)
      if lObj.isNull() then
        None
      else
        Some(extractCid(lObj))
    else
      None

    // Parse entries array
    val entriesArray = cbor.get("e")
    val entriesCount = entriesArray.size()

    val entries = new Array[(String, MstEntry)](entriesCount)
    var prevKey = Array.emptyByteArray

    var i = 0
    while i < entriesCount do
      val entryObj = entriesArray.get(i)
      val entry = MstEntry.fromCBOR(entryObj)

      // Reconstruct full key
      val fullKey = entry.reconstructKey(prevKey)
      val keyStr = new String(fullKey, "UTF-8")

      entries(i) = (keyStr, entry)
      prevKey = fullKey
      i += 1

    MstNode(leftLink, entries)
