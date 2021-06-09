val scala3Version = "2.13.6"

lazy val root = project
    .in(file("."))
    .settings(
        name := "pinecone-core",
        version := "0.1.0",

        scalaVersion := scala3Version,
        libraryDependencies ++= Seq(
            "com.github.pureconfig" %% "pureconfig" % "0.15.0",
            "com.zaxxer" % "HikariCP" % "4.0.3",
            "org.playframework.anorm" %% "anorm" % "2.6.10",
            "org.postgresql" % "postgresql" % "42.2.14",
            "net.snowflake" % "snowflake-jdbc" % "3.13.4",
            "com.microsoft.sqlserver" % "mssql-jdbc" % "7.0.0.jre8",
            "com.amazon.redshift" % "redshift-jdbc42" % "2.0.0.4",
            "com.ibm.db2.jcc" % "db2jcc" % "db2jcc4",
             "org.scalactic" %% "scalactic" % "3.2.9",
            "org.scalatest" %% "scalatest" % "3.2.9" % "test",
            "org.scalatest" %% "scalatest-flatspec" % "3.2.9" % "test"
        )
    )
