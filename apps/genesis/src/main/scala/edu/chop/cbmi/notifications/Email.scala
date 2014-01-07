package edu.chop.cbmi.notifications

import java.util.Properties
import javax.mail.{Transport, Message, Session}
import javax.mail.internet.{InternetAddress, MimeMessage}
import edu.chop.cbmi.util.ConfigUtil

/**
 * Created with IntelliJ IDEA.
 * User: masinoa
 * Date: 6/11/13
 * Time: 11:24 AM
 * To change this template use File | Settings | File Templates.
 */
object Email {
  private val config = ConfigUtil.applicationConfig("notifications")
  private val senderConfig = config.getConfig("sender")
  val recipients = config.getStringList("recipients")
  val user = senderConfig.getString("username")
  val pw = senderConfig.getString("password")
  val smtp = senderConfig.getString("smtp")
  val port = senderConfig.getString("port")
  val auth = senderConfig.getString("auth")
  val starttlsEnable = senderConfig.getString("starttlsEnable")

  val props = new Properties()
  props.put("mail.smtp.auth", auth);
  props.put("mail.smtp.starttls.enable", starttlsEnable);
  props.put("mail.smtp.host", smtp);
  props.put("mail.smtp.port", port);

  def sendEmail(subject: String, msg: String) = {

    val authenticator = new GenesisAuthenticator(user, pw)
    val session = Session.getInstance(props, authenticator)
    try{
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(user))
      recipients.toArray.foreach{r => message.addRecipient(Message.RecipientType.TO,
        new InternetAddress(r.toString))}
      message.setSubject(subject)
      message.setText(msg)
      Transport.send(message)
    } catch {
      case e:Exception => println(e)
    }
  }

  def notify_?() = {
    config.getBoolean("sendNotifications")
  }
}

class GenesisAuthenticator(user:String, pw:String) extends javax.mail.Authenticator{
  override protected def getPasswordAuthentication() ={
    new javax.mail.PasswordAuthentication(user,pw)
  }
}
