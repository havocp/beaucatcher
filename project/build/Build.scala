import sbt._
import reaktor.scct.ScctProject

class BeaucatcherProject(info: ProjectInfo) extends ParentProject(info) {
    override def parallelExecution = true
    override def ivyUpdateLogging = UpdateLogging.Full 

    /* Repositories */
    val akkaRepo = "Akka Repo" at "http://akka.io/repository"
    val scalaToolsSnapshots = "Scala Tools Snapshots" at "http://scala-tools.org/repo-snapshots/"

    /* Subprojects */
    lazy val bson = project("bson", "beaucatcher-bson", new BeaucatcherBSONProject(_))
    lazy val mongo = project("mongo", "beaucatcher-mongo", new BeaucatcherMongoProject(_), bson)
    lazy val async = project("async", "beaucatcher-async", new BeaucatcherAsyncProject(_), mongo)
    lazy val casbah = project("casbah", "beaucatcher-casbah", new BeaucatcherCasbahProject(_), mongo)
    lazy val hammersmith = project("hammersmith", "beaucatcher-hammersmith", new BeaucatcherHammersmithProject(_), async)

    class BeaucatcherBSONProject(info: ProjectInfo) extends DefaultProject(info) with ScctProject {
        val scalajCollection = "org.scalaj" %% "scalaj-collection" % "1.1"
        val scalap = "org.scala-lang" % "scalap" % "2.9.0-1"
        val commonsCodec = "commons-codec" % "commons-codec" % "1.4"
        val casbahCore = "com.mongodb.casbah" %% "casbah-core" % "2.1.5-1"

        val junitInterface = "com.novocode" % "junit-interface" % "0.7" % "test->default"
        val liftJson = "net.liftweb" %% "lift-json" % "2.4-SNAPSHOT" % "test->default"
    }

    class BeaucatcherMongoProject(info: ProjectInfo) extends DefaultProject(info) with ScctProject {
    }

    class BeaucatcherAsyncProject(info: ProjectInfo) extends DefaultProject(info) with ScctProject {
        val akkaActor = "se.scalablesolutions.akka" % "akka-actor" % "1.1"
    }

    class BeaucatcherCasbahProject(info: ProjectInfo) extends DefaultProject(info) with ScctProject {
    }

    class BeaucatcherHammersmithProject(info: ProjectInfo) extends DefaultProject(info) with ScctProject {
        val hammersmith = "com.mongodb.async" %% "mongo-driver" % "0.2.0"

        /* Repositories */
        val twttrRepo = "Twitter Public Repo" at "http://maven.twttr.com"
    }
}
