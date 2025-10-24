package com.mchange.bskyarchive

import java.io.InputStream

class CachingBskyRepo( is : InputStream  ) extends AbstractBskyRepo(is):
  override val blocks : LazyList[CarBlock] =
    def next : LazyList[CarBlock] =
      parseBlock match
        case Some( carBlock ) => carBlock #:: next
        case None             => LazyList.empty
    next

