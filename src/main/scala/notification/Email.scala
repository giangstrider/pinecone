package notification

import configuration.Pinecone.pineconeConf
import org.apache.commons.mail._

import scala.language.implicitConversions

sealed abstract class MailType
case object Plain extends MailType
case object Rich extends MailType


case class Mail(
    from: (String, String),
    to: Seq[String],
    cc: Seq[String] = Seq.empty,
    bcc: Seq[String] = Seq.empty,
    subject: String,
    message: String,
    richMessage: Option[String] = None
)

object Email {
	private val emailConf = pineconeConf.notification.emailConf.get

	implicit def stringToSeq(single: String): Seq[String] = Seq(single)
	implicit def liftToOption[T](t: T): Option[T] = Some(t)

	def send(mail: Mail): Unit = {
		val format = if (mail.richMessage.isDefined) Rich else Plain

		val mailObject = format match {
			case Plain => new SimpleEmail().setMsg(mail.message)
            case Rich => new HtmlEmail().setHtmlMsg(mail.richMessage.get).setTextMsg(mail.message)
		}

		mail.to foreach mailObject.addTo
		mail.cc foreach mailObject.addCc
		mail.bcc foreach mailObject.addBcc

		mailObject.setHostName(emailConf.smtpServer)
		mailObject.setFrom(mail.from._1, mail.from._2).setSubject(mail.subject).send
	}
}
