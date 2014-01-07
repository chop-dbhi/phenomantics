package edu.chop.cbmi.util

import java.io.File
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.model.PutObjectRequest

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 6/19/13
 * Time: 12:44 PM
 */
object S3 {

  lazy val awsConfig = ConfigUtil.applicationConfig("AWS")

  lazy val awsAccessKeyId = awsConfig.getString("AccessKeyId")
  lazy val awsSecretKey = awsConfig.getString("AccessKeySecret")

  lazy private val s3Config = awsConfig.getConfig("S3")
  val defaultBucketName = s3Config.getString("name")
  val use_S3_? = s3Config.getString("use").trim.toLowerCase == "true"

  //lazy private val s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider())

  lazy val awsCredentials = new AWSCredentials {
    def getAWSAccessKeyId(): String = awsAccessKeyId

    def getAWSSecretKey(): String = awsSecretKey
  }

  def putFileInBucket(file: File, key: String, bucketName:String = defaultBucketName) = {
    val s3 = new AmazonS3Client(awsCredentials)
    s3.putObject(new PutObjectRequest(bucketName, key, file))
  }
}
