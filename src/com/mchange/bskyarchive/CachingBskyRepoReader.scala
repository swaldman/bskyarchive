package com.mchange.bskyarchive

import java.io.InputStream

class CachingBskyRepoReader( is : InputStream  ) extends AbstractBskyRepoReader(is):
  override val blocks : LazyList[CarBlock] =
    def next : LazyList[CarBlock] =
      CarBlock.read(is) match
        case Some( carBlock ) => carBlock #:: next
        case None => LazyList.empty
    next

