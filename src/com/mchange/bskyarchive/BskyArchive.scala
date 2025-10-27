package com.mchange.bskyarchive

import java.io.RandomAccessFile
import scala.util.Using
import java.io.BufferedInputStream
import java.io.FileInputStream
import com.mchange.bskyarchive.exception.BadBskyRepo

object BskyArchive:
  trait Navigator extends AutoCloseable:
    def getBlock( cid : Cid ) : Option[CarBlock]
class BskyArchive( path : os.Path ):
  val ( header, commits, offsetMap, profile ) : (CarHeader, List[CarBlock.Commit], Map[String,Long], CarBlock.Record) =
    Using.resource(new RandomAccessFile(path.toIO, "r")): raf =>
      Using.resource(new BufferedInputStream( new FileInputStream( raf.getFD() ) ) ): is =>
        Using.resource(BskyRepoReader(is)): repo =>
          val h = repo.header
          var cs : List[CarBlock.Commit] = Nil
          var p : CarBlock.Record = null
          val mapBuider = Map.newBuilder[String,Long]
          var offset = raf.getFilePointer()
          while repo.blocks.hasNext do
            val block = repo.blocks.next()
            block match
              case commit : CarBlock.Commit =>
                cs = commit :: cs
              case rec : CarBlock.Record if rec.`type` == "app.bsky.actor.profile" =>
                if p != null then
                  throw new BadBskyRepo(s"Duplicate profile records! $p $rec")
                p = rec
              case _ =>
                /* ignore */
            mapBuider += Tuple2( block.cid.toMultibaseCidBase32, offset )
            offset = raf.getFilePointer()
          (h, cs, mapBuider.result(), p)

  if commits.isEmpty then
    throw new BadBskyRepo("No commit records found in repo, can't identify did (distributed id).")

  if profile == null then
    throw new BadBskyRepo("No profile found in repo!")

  val did = commits.head.did

  /**
   * Build maps between TIDs and CIDs by parsing all MST nodes.
   */
  lazy val (cidToTid, tidToCid): (Map[Cid, String], Map[String, Cid]) =
    val tidBuilder = Map.newBuilder[Cid, String]
    val cidBuilder = Map.newBuilder[String, Cid]
    Using.resource(new BufferedInputStream(os.read.inputStream(path))): is =>
      Using.resource(new BskyRepoReader(is)): brr =>
        brr.blocks.foreach:
          case node: CarBlock.Node =>
            // Extract all TID -> CID mappings from this MST node
            // MST keys are full paths like "app.bsky.feed.post/3l3r37udbxt2f"
            // We want just the TID (rkey) part after the last slash
            node.mst.entries.foreach { case (mstKey, entry) =>
              val tid = mstKey.substring(mstKey.lastIndexOf('/') + 1)
              tidBuilder += (entry.value -> tid)
              cidBuilder += (tid -> entry.value)
            }
          case _ =>
            // Not an MST node, skip
            ()
    (tidBuilder.result(), cidBuilder.result())

  def newNavigator() : BskyArchive.Navigator = new BskyArchive.Navigator:
    val raf = new RandomAccessFile(path.toIO, "r")
    def getBlock( cidBase32 : String ) : Option[CarBlock] =
      offsetMap.get(cidBase32).flatMap: offset =>
        raf.seek( offset )
        Using.resource(new BufferedInputStream(new FileInputStream(raf.getFD))): is =>
          CarBlock.read(is)
    def getBlock( cid : Cid ) : Option[CarBlock] = getBlock(cid.toMultibaseCidBase32)
    override def close() : Unit = raf.close()
  end newNavigator

  private def collectRecords( pf : PartialFunction[CarBlock,CarBlock.Record] ) : Seq[CarBlock.Record] =
    Using.resource(new BufferedInputStream(os.read.inputStream(path))): is =>
      Using.resource(new BskyRepoReader(is)): brr =>
        brr.blocks
          .collect(pf)
          .toVector

  def records() : Seq[CarBlock.Record] =
    collectRecords {
      case rec : CarBlock.Record => rec
    }

  private def recordsbyType( `type` : String ) : Seq[CarBlock.Record] =
    collectRecords {
      case rec : CarBlock.Record if rec.`type` == `type` => rec
    }

  // (app.bsky.feed.post, chat.bsky.actor.declaration, app.bsky.graph.listitem, app.bsky.graph.follow, app.bsky.graph.list, app.bsky.feed.like, app.bsky.actor.profile, app.bsky.feed.repost)


  def posts() : Seq[CarBlock.Record] = recordsbyType("app.bsky.feed.post")

  def allImageUrls() : Set[String] = posts().flatMap( _.blobRefs ).filter( _.mimeType.startsWith("image") ).map( _.toBskyUrl(did) ).toSet

  /**
   * Get the TID (record key) for a given record CID.
   * Returns None if the CID is not found in any MST node.
   */
  def getTid(recordCid: Cid): Option[String] =
    cidToTid.get(recordCid)

  /**
   * Get the TID for a record.
   */
  def getTid(record: CarBlock.Record): Option[String] =
    getTid(record.cid)

  /**
   * Get the CID for a given TID (record key).
   * Returns None if the TID is not found.
   */
  def getCid(tid: String): Option[Cid] =
    tidToCid.get(tid)

  /**
   * Get a record by its TID (record key).
   * Returns None if the TID is not found or the block cannot be read.
   */
  def getRecordByTid(tid: String): Option[CarBlock.Record] =
    Using.resource(newNavigator()): nav =>
      getCid(tid).flatMap: cid =>
        nav.getBlock(cid) match
          case Some(record: CarBlock.Record) => Some(record)
          case _ => None
