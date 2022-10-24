import kotlinx.cinterop.*
import platform.posix.*

fun readStringFromFile(file: String): String = memScoped {
    val st = alloc<stat>()
    stat(file, st.ptr)
    val size = st.st_size
    val buf = allocArray<ByteVar>(size)
    val f = fopen(file, "rb")
    fread(buf, 1UL, size.toULong(), f)
    fclose(f)
    buf.toKString()
}

fun writeStringToFile(file: String, v: String): Boolean = memScoped{
    val f = fopen(file, "wb")
    val buf = v.encodeToByteArray().toCValues()
    val ret = fwrite(buf, buf.size.toULong(), 1, f)
    fclose(f)
    ret == 0UL
}

fun getFileSize(file: String): Long = memScoped {
    val st = alloc<stat>()
    stat(file, st.ptr)
    st.st_size
}

fun isFileExists(file: String): Boolean = memScoped {
    access(file, 0) == 0
}