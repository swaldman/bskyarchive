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

  def newNavigator() : BskyArchive.Navigator = new BskyArchive.Navigator:
    val raf = new RandomAccessFile(path.toIO, "r")
    def getBlock( cid : Cid ) : Option[CarBlock] =
      offsetMap.get(cid.toMultibaseCidBase32).flatMap: offset =>
        raf.seek( offset )
        Using.resource(new BufferedInputStream(new FileInputStream(raf.getFD))): is =>
          CarBlock.read(is)
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
