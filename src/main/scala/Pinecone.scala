import configuration.Pinecone.pineconeConf
import controller.ApplicationController
import interface.Command
import reconciliation.Executor
import parser.PineconeSQLProcessor

object Pinecone {

  def main(args: Array[String]): Unit = {
    init

    //println(PineconeSQLProcessor("select * from data where $WHERE{YEAR(GET_DATE(1))} and ${GET_DATE(-2)}"))
//    ApplicationController.init()
    Command.Reconciliation.reconcile
//    println(PineconeSQLProcessor(
//      """
//       | select to_date(conv) as "TradingDate", sum(OrderTotal), count(*)
//       |from (select *, do_created as conv from KSFPA.OMS.CUSTOMERORDER
//       | where year(do_created) $TIME{IN(YEAR(GET_DATE(-1)),YEAR(GET_DATE(-1000)))}
//       |) where  "TradingDate" $TIME{IN(GET_DATE(-1), GET_DATE(-1000))}
//       |group by "TradingDate" order by "TradingDate"""".stripMargin))
//    println(PineconeSQLProcessor(
//      """
//       | select to_date(conv) as "TradingDate", sum(OrderTotal), count(*)
//       |from (select *, do_created as conv from KSFPA.OMS.CUSTOMERORDER
//       | where year(do_created) $TIME{BETWEEN('2021-01-12', YEAR(GET_DATE(-1)))}
//       |) where  "TradingDate" $TIME{IN(GET_DATE(-1), GET_DATE(-1000), GET_DATE(-2000))}
//       |group by "TradingDate" order by "TradingDate"""".stripMargin))
  }

  def init {
    val proxyConf = pineconeConf.proxy
    if (proxyConf.useProxy) {
      System.setProperty("http.proxyHost", proxyConf.httpProxy.get)
      System.setProperty("http.proxyPort", proxyConf.httpPort.get)
      System.setProperty("https.proxyHost", proxyConf.httpsProxy.get)
      System.setProperty("https.proxyPort", proxyConf.httpsPort.get)
    }
  }
}
