package com.whitepages.framework.client.riak

import java.lang.annotation.{Retention, RetentionPolicy}

import com.basho.riak.client.api.annotations._
import com.basho.riak.client.api.cap.VClock
import com.basho.riak.client.core.query.indexes.RiakIndexes
import com.basho.riak.client.core.query.links.RiakLinks

import scala.annotation.meta.field
import scala.beans.BeanProperty

// http://www.codecommit.com/blog/java/interop-between-java-and-scala

/*
Case classes require the @field meta (http://piotrbuda.eu/2012/10/scala-case-classes-and-annotations-part-1.html)
 */


@Retention(RetentionPolicy.RUNTIME) // not sure it is needed here
case class DomainObject[T]( value: Option[T]
                            , @(RiakBucketName@field) @BeanProperty var bucketName: String=null /* Java API */
                            , @(RiakBucketType@field) @BeanProperty var bucketType: String=null /* Java API */
                            , @(RiakContentType@field) @BeanProperty var contentType: String=null /* Java API */
                            //FIXME: Invalid usage of @RiakIndex annotation.
                            //, @(RiakIndex@field) @BeanProperty var indexes: RiakIndexes=null /* Java API */
                            , @(RiakKey@field) @BeanProperty var key: String=null /* Java API */
                            , @(RiakLastModified@field) @BeanProperty var lastModified: Long=0L /* Java API */
                            , @(RiakLinks@field) @BeanProperty var links: RiakLinks=null /* Java API */
                            , @(RiakTombstone@field) @BeanProperty var tombstone: Boolean=java.lang.Boolean.FALSE /* Java API */
                            , @(RiakUsermeta@field) @BeanProperty var userMeta: java.util.Map[String, String]=null /* Java API */
                            , @(RiakVClock@field) @BeanProperty var vclock: VClock=null /* Java API */
                            , @(RiakVTag@field) @BeanProperty var vtag: String=null /* Java API */
                            ) {

  // This is to generate parameterless ctor that Riak client needs to represent deleted objects
  def this() = this(None)

  def copyWithValue(v: Option[T]): DomainObject[T] = {

    def deepCopyJavaMap(javaMap: java.util.Map[String, String]): java.util.Map[String, String] = {
      if (null == javaMap) null
      else {
        val clone = new java.util.HashMap[String, String]()
        clone.putAll(javaMap)
        clone
      }
    }

    DomainObject(value = v
      , bucketName = this.bucketName
      , bucketType = this.bucketType
      , contentType = this.contentType
      //, indexes = this.indexes
      , key = this.key
      , lastModified = this.lastModified
      , links = this.links
      , tombstone = this.tombstone
      , userMeta = deepCopyJavaMap(this.userMeta)
      , vclock = this.vclock
      , vtag = this.vtag
    )
  }
}
