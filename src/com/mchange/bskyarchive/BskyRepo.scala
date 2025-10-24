package com.mchange.bskyarchive

import java.io.InputStream

class BskyRepo( is : InputStream  ) extends AbstractBskyRepo(is):
  override val blocks : Iterator[CarBlock] = Iterator.continually(parseBlock).takeWhile(_.isDefined).map(_.get)

