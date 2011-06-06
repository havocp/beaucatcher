import sbt._

class BeaucatcherProject(info: ProjectInfo) extends ParentProject(info) {
    override def parallelExecution = true

    /* Repositories */
    val akkaRepo = "Akka Repo" at "http://akka.io/repository"
    val scalaToolsSnapshots = "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/"

    /* Subprojects */
    lazy val bson = project("bson", "beaucatcher-bson", new BeaucatcherBSONProject(_))
    lazy val mongo = project("mongo", "beaucatcher-mongo", new BeaucatcherMongoProject(_), bson)
    lazy val async = project("async", "beaucatcher-async", new BeaucatcherAsyncProject(_), mongo)
    lazy val casbah = project("casbah", "beaucatcher-casbah", new BeaucatcherCasbahProject(_), mongo)
    lazy val hammersmith = project("hammersmith", "beaucatcher-hammersmith", new BeaucatcherHammersmithProject(_), async)

    class BeaucatcherBSONProject(info: ProjectInfo) extends DefaultProject(info) {
        val scalajCollection = "org.scalaj" %% "scalaj-collection" % "1.1"
        val liftJson = "net.liftweb" %% "lift-json" % "2.4-SNAPSHOT"
        val scalap = "org.scala-lang" % "scalap" % "2.9.0-1"
        val commonsCodec = "commons-codec" % "commons-codec" % "1.4"
        val casbahCore = "com.mongodb.casbah" %% "casbah-core" % "2.1.5-1"

        val junitInterface = "com.novocode" % "junit-interface" % "0.7" % "test->default"
    }

    class BeaucatcherMongoProject(info: ProjectInfo) extends DefaultProject(info) {
    }

    class BeaucatcherAsyncProject(info: ProjectInfo) extends DefaultProject(info) {
        val akkaActor = "se.scalablesolutions.akka" % "akka-actor" % "1.1"
    }

    class BeaucatcherCasbahProject(info: ProjectInfo) extends DefaultProject(info) {
    }

    class BeaucatcherHammersmithProject(info: ProjectInfo) extends DefaultProject(info) {
        val casbah = "com.mongodb.casbah" %% "casbah-util" % "2.2.0-SNAPSHOT"
        val commonsPool = "commons-pool" % "commons-pool" % "1.5.5"
        val netty = "org.jboss.netty" % "netty" % "3.2.4.Final"
        val twitterUtilCore = "com.twitter" % "util-core" % "1.8.13"
        val slf4j = "org.slf4j" % "slf4j-api" % "1.6.1"
        val logback = "ch.qos.logback" % "logback-classic" % "0.9.28"

        /* Repositories */
        val jbossRepo = "JBoss Public Repo" at "https://repository.jboss.org/nexus/content/groups/public-jboss/"
        val twttrRepo = "Twitter Public Repo" at "http://maven.twttr.com"
    }
}
