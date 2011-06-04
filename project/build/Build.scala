import sbt._

class BeaucatcherProject(info: ProjectInfo) extends ParentProject(info) {
    override def parallelExecution = true

    /* Repositories */
    val akkaRepo = "Akka Repo" at "http://akka.io/repository"

    /* Subprojects */
    lazy val bson = project("bson", "beaucatcher-bson", new BeaucatcherBSONProject(_))
    lazy val mongo = project("mongo", "beaucatcher-mongo", new BeaucatcherMongoProject(_), bson)
    lazy val akka = project("akka", "beaucatcher-akka", new BeaucatcherAkkaProject(_), mongo)
    lazy val casbah = project("casbah", "beaucatcher-casbah", new BeaucatcherCasbahProject(_), mongo)
    lazy val hammersmith = project("hammersmith", "beaucatcher-hammersmith", new BeaucatcherHammersmithProject(_), akka)

    class BeaucatcherBSONProject(info: ProjectInfo) extends DefaultProject(info) {
        val scalajCollection = "org.scalaj" % "scalaj-collection_2.8.0" % "1.0"
        val liftJson = "net.liftweb" %% "lift-json" % "2.3"
        val scalap = "org.scala-lang" % "scalap" % "2.8.1"
        val commonsCodec = "commons-codec" % "commons-codec" % "1.4"
        val casbahCore = "com.mongodb.casbah" %% "casbah-core" % "2.1.2"

        val junitInterface = "com.novocode" % "junit-interface" % "0.7" % "test->default"
    }

    class BeaucatcherMongoProject(info: ProjectInfo) extends DefaultProject(info) {
    }

    class BeaucatcherAkkaProject(info: ProjectInfo) extends DefaultProject(info) {
        val akkaActor = "se.scalablesolutions.akka" % "akka-actor" % "1.0"
    }

    class BeaucatcherCasbahProject(info: ProjectInfo) extends DefaultProject(info) {
    }

    class BeaucatcherHammersmithProject(info: ProjectInfo) extends DefaultProject(info) {
    }
}
