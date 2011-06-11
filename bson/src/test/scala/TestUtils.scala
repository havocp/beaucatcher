
abstract trait TestUtils {
    protected def intercept[E <: Throwable : Manifest](block : => Unit) : E = {
        val expectedClass = manifest.erasure.asInstanceOf[Class[E]]
        var thrown : Option[Throwable] = None
        try {
            block
        } catch {
            case t : Throwable => thrown = Some(t)
        }
        thrown match {
            case Some(t) if expectedClass.isAssignableFrom(t.getClass) =>
                t.asInstanceOf[E]
            case Some(t) =>
                throw new Exception("Expected exception %s was not thrown, got %s".format(expectedClass.getName, t), t)
            case None =>
                throw new Exception("Expected exception %s was not thrown".format(expectedClass.getName))
        }
    }
}
