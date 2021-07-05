import configuration.Pinecone.pineconeConf
import deploy.Deployer

object Pinecone {
	def main(args : Array[String]): Unit = {
		init
		Deployer.deploy
	}

	def init {
		val proxyConf = pineconeConf.proxy
		if(proxyConf.useProxy) {
			System.setProperty("http.proxyHost", proxyConf.httpProxy.get)
			System.setProperty("http.proxyPort", proxyConf.httpPort.get)
			System.setProperty("https.proxyHost", proxyConf.httpsProxy.get)
			System.setProperty("https.proxyPort", proxyConf.httpsPort.get)
		}
	}
}
