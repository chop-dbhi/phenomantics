package edu.chop.cbmi.util

import com.amazonaws.services.ec2.AmazonEC2Client
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.ec2.model.TerminateInstancesRequest

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 6/20/13
 * Time: 9:16 AM
 */
object EC2 {
  lazy val awsConfig = ConfigUtil.applicationConfig("AWS")

  lazy val awsAccessKeyId = awsConfig.getString("AccessKeyId")
  lazy val awsSecretKey = awsConfig.getString("AccessKeySecret")

  lazy private val ec2Config = awsConfig.getConfig("EC2")

  lazy val terminate_instances_? = ec2Config.getString("terminate").trim.toLowerCase == "true"

  lazy val awsCredentials = new AWSCredentials {
    def getAWSAccessKeyId(): String = awsAccessKeyId

    def getAWSSecretKey(): String = awsSecretKey
  }

  lazy private val genesisId = ec2Config.getString("genesisInstanceId")

  lazy private val instanceIds = {
    val idsIter= ec2Config.getStringList("instanceIDS").iterator()
    val l = new java.util.ArrayList[String]()
    while(idsIter.hasNext){
      val n = idsIter.next()
      if(n!=genesisId)l.add(n)
    }
    l
  }

  def terminateAllInstances() = {
    val ec2 = new AmazonEC2Client(awsCredentials)
    if(!instanceIds.isEmpty)ec2.terminateInstances(new TerminateInstancesRequest(instanceIds))
    //terminate genesis instance last
    val l = new java.util.ArrayList[String]()
    l.add(genesisId)
    ec2.terminateInstances(new TerminateInstancesRequest(l))
  }
}
