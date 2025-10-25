package com.mchange.bskyarchive

import java.io.InputStream

class BskyRepoReader( is : InputStream  ) extends AbstractBskyRepoReader(is):
  override val blocks : Iterator[CarBlock] = Iterator.continually(CarBlock.read(is)).takeWhile(_.isDefined).map(_.get)

