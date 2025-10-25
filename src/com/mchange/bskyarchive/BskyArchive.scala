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

  val ( header, commits, offsetMap ) : (CarHeader, List[CarBlock], Map[String,Long]) =
    Using.resource(new RandomAccessFile(path.toIO, "r")): raf =>
      Using.resource(new BufferedInputStream( new FileInputStream( raf.getFD() ) ) ): is =>
        Using.resource(BskyRepoReader(is)): repo =>
          val h = repo.header
          var cs : List[CarBlock] = Nil
          val mapBuider = Map.newBuilder[String,Long]
          var offset = raf.getFilePointer()
          while repo.blocks.hasNext do
            val block = repo.blocks.next()
            if block.`type` == CarBlock.Type.Commit then
              cs = block :: cs
            mapBuider += Tuple2( block.cid.toMultibaseCidBase32, offset )
            offset = raf.getFilePointer()
          (h, cs, mapBuider.result())

  if commits.isEmpty then
    throw new BadBskyRepo("No commit records found in repo, can't identify did (distributed id).")

  val did = commits.head.cbor.get("did").AsString()

  def newNavigator() : BskyArchive.Navigator = new BskyArchive.Navigator:
    val raf = new RandomAccessFile(path.toIO, "r")

    def getBlock( cid : Cid ) : Option[CarBlock] =
      offsetMap.get(cid.toMultibaseCidBase32).flatMap: offset =>
        raf.seek( offset )
        val is = new BufferedInputStream(new FileInputStream(raf.getFD))
        try
          CarBlock.read(is)
        finally
          is.close()

    override def close() : Unit = raf.close()
  end newNavigator


