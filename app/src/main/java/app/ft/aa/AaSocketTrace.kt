package app.ft.aa

import app.ft.core.DiagLog
import java.io.FilterInputStream
import java.io.FilterOutputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Diagnostic wrappers for the Android Auto developer-server TCP socket.
 * They do not change framing, buffering, or socket lifecycle behaviour.
 */
internal object AaSocketTrace {
    fun input(socket: Socket, sessionId: Long, source: String) =
        object : FilterInputStream(socket.getInputStream()) {
            private val eofLogged = AtomicBoolean(false)
            private fun state() =
                "local=${socket.localSocketAddress} peer=${socket.remoteSocketAddress} " +
                    "connected=${socket.isConnected} closed=${socket.isClosed} " +
                    "inputShutdown=${socket.isInputShutdown} outputShutdown=${socket.isOutputShutdown}"

            override fun read(): Int {
                val value = super.read()
                if (value < 0) logEof()
                return value
            }

            override fun read(buffer: ByteArray, offset: Int, length: Int): Int {
                val count = super.read(buffer, offset, length)
                if (count < 0) logEof()
                return count
            }

            private fun logEof() {
                if (eofLogged.compareAndSet(false, true)) {
                    DiagLog.w("AA", "TCP peer EOF session #$sessionId ($source): ${state()}")
                }
            }

            override fun close() {
                DiagLog.i("AA", "local input.close session #$sessionId ($source): ${state()} caller=${caller()}")
                super.close()
            }
        }

    fun output(socket: Socket, sessionId: Long, source: String) =
        object : FilterOutputStream(socket.getOutputStream()) {
            override fun close() {
                DiagLog.i(
                    "AA",
                    "local output.close session #$sessionId ($source): " +
                        "local=${socket.localSocketAddress} peer=${socket.remoteSocketAddress} " +
                        "closed=${socket.isClosed} caller=${caller()}"
                )
                super.close()
            }
        }

    private fun caller(): String =
        Throwable().stackTrace.drop(1).take(6).joinToString(" <- ") {
            "${it.className.substringAfterLast('.')}.${it.methodName}:${it.lineNumber}"
        }
}
