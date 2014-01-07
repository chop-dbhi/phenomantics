package edu.chop.cbmi.util

import java.io.File
import com.typesafe.config.ConfigFactory

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 6/7/13
 * Time: 12:25 PM
 * To change this template use File | Settings | File Templates.
 */
object ConfigUtil {
  private lazy val configFile = new File("./conf/application.conf")

  def applicationConfig(path:String) = ConfigFactory.parseFile(configFile).getConfig(path)

}
