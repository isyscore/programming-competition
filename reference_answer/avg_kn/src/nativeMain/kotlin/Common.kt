import kotlinx.coroutines.*
import kotlin.DslMarker
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

@Target(AnnotationTarget.ANNOTATION_CLASS)
@Retention(AnnotationRetention.BINARY)
@MustBeDocumented
@SinceKotlin("1.1")
annotation class DslMarker

@DslMarker
@Target(AnnotationTarget.CLASS, AnnotationTarget.TYPEALIAS, AnnotationTarget.TYPE, AnnotationTarget.FUNCTION)
annotation class KtDsl

@KtDsl
fun routine(dispatcher: CoroutineDispatcher = Dispatchers.Default, parameters: Any? = null, block: suspend CoroutineScope.(Any?) -> Unit): Pair<CoroutineScope, Job> {
    val scope = CoroutineScope(dispatcher)
    val job = scope.launch { block(parameters) }
    return scope to job
}

@KtDsl
fun CoroutineScope.sub(
    parameters: Any? = null,
    block: suspend CoroutineScope.(Any?) -> Unit
): Job {
    return async { block(parameters) }
}