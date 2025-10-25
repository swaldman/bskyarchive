package com.mchange.bskyarchive.exception

class BskyArchiveException( message : String, cause : Throwable = null ) extends Exception( message, cause )

final class BadBskyRepo( message : String, cause : Throwable = null ) extends BskyArchiveException( message, cause )
final class UnexpectedRecordType( message : String, cause : Throwable = null ) extends BskyArchiveException( message, cause )

