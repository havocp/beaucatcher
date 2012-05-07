package org.beaucatcher.mongo

// This cache is invalidated whenever a new Context is used, but
// is never cleared (we'll never GC the most recent item).
// If this is ever an issue in practice we could have some
// sort of destroy listeners on Context, but doesn't seem
// at all worth it for now.
private[mongo] class ContextCache[T](val creator: (Context) => T) {
    @volatile private var cache: Option[(Context, T)] = None
    def get(implicit context: Context): T = {
        cache match {
            case Some((cachedContext, t)) if (cachedContext eq context) =>
                t
            case _ =>
                // there's an obvious race here where we may create
                // for the same context more than once and one of them
                // will "win"; that's fine. cache miss is not a disaster,
                // however it would be a disaster if we returned something
                // for the wrong context
                val t = creator(context)
                cache = Some((context, t))
                t
        }
    }
}

private[mongo] object ContextCache {
    def apply[T](creator: (Context) => T) = new ContextCache(creator)
}
