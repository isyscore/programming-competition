import kotlinx.cinterop.*
import platform.posix.*
import kotlin.native.concurrent.freeze

//const val FILENAME = "./small_int_set"
const val FILENAME = "./huge_int_set"
const val INDEXFILE = "./index"

var routineCount = 10

data class RoutingArgs(val idx: Int, val current: Long, val limit: Long)
data class AvgNums(val n: Double, val avg: Double)

fun main(args: Array<String>) {
    if (args.size == 2) {
        if (args[0] == "index") {
            buildIndex(args[1].toInt())
        }
        return
    }
    if (args.size == 1) {
        routineCount = args[0].toInt()
    }

    var useIndex = false
    val indexes = mutableListOf<Long>()

    if (isFileExists(INDEXFILE)) {
        val list = readStringFromFile(INDEXFILE).split("\n").filter {
            it.trim() != ""
        }.map {
            it.toLong()
        }
        indexes.addAll(list)
        useIndex = true
        routineCount = indexes.size - 1
    }

    val totalSize = getFileSize(FILENAME)
    val blockSize =  ceil(totalSize.toDouble() / routineCount.toDouble()).toLong()

    val startTime = currentTimeMillis()

    var av = 0.0
    var n = 1.0

    var current = 0L
    var limitSize: Long
    val tids = mutableListOf<pthread_t?>()

    for (i in 0 until routineCount) {
        limitSize = if (useIndex)  indexes[i+1] - indexes[i]  else  blockSize
        if (limitSize <= 0) {
            break
        }

        val ref = RoutingArgs(i, current, limitSize).freeze()
        val sref = StableRef.create(ref)
        val ptr = sref.asCPointer()

        memScoped {
            val tid = alloc<pthread_tVar>()
            pthread_create(tid.ptr, null, staticCFunction(::thread_read), ptr)
            tids.add(tid.value)
        }

        if (useIndex) {
            current = indexes[i+1]
        } else {
            current += blockSize + 1
        }
    }

    for (i in 0 until routineCount) {
        memScoped {
            val p = alloc<COpaquePointerVar>()
            pthread_join(tids[i], p.ptr)
            val sref = p.value!!.asStableRef<AvgNums>()
            val ref = sref.get()

            av = (n-1.0)/(n+ref.n-1.0)*av + ref.avg*(ref.n/(n+ref.n-1.0))
            n += ref.n

            sref.dispose()
        }
    }

    val endTime = currentTimeMillis()
    println("numbers = ${(n-1).toLong()}, avg = $av time = ${endTime - startTime}")
}

fun thread_read(arg: COpaquePointer?): COpaquePointer? {

    val sref = arg!!.asStableRef<RoutingArgs>()
    val ref = sref.get()

    var _av = 0.0
    var _n = 1.0

    val file = fopen(FILENAME, "rb")
    fseek(file, ref.current, 0)
    var cummulativeSize = 0L
    while (true) {
        if (cummulativeSize >= ref.limit) break
        memScoped {
            val b = allocArray<ByteVar>(18)
            fgets(b, 18, file)
            val ks = b.toKString()
            cummulativeSize += ks.length
            val f = ks.trim().toDouble()
            _av = (_n - 1) / _n * _av + f / _n
            _n += 1.0
        }
        if (feof(file) != 0) break
    }
    fclose(file)
    sref.dispose()

    _n--

    val retRef = AvgNums(_n, _av).freeze()
    val sretRef = StableRef.create(retRef)
    return sretRef.asCPointer()
}

fun buildIndex(rc: Int) {
    val totalSize = getFileSize(FILENAME)
    val blockSize = ceil(totalSize.toDouble() / rc.toDouble()).toLong()

    val file = fopen(FILENAME, "r")
    val sp = mutableListOf<Long>()
    sp.add(0)
    var current = blockSize
    for (i in 1 until rc) {
        fseek(file, current, 0)
        var offset = 0L
        var hasReturn = false
        while (true) {
            val b = fgetc(file)
            if (feof(file) != 0) {
                break
            }
            offset++
            if (b == 10) {
                // 如果读到一个回车，直接记录上一个的结束位置
                sp.add(current + offset)
                hasReturn = true
                break
            }
        }
        if (!hasReturn) {
            // 一行到最后都没读到回车，也算是一行
            sp.add(current + offset + 1)
        }
        current +=  offset + blockSize + 1
        if (current >=  totalSize) {
            break
        }
    }

    sp.add(totalSize)
    fclose(file)

    val idxStr = sp.joinToString("\n") { it.toString()}
    writeStringToFile(INDEXFILE, idxStr)
}
