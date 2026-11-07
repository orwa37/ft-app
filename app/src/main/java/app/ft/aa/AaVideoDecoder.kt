package app.ft.aa

import android.media.MediaCodec
import android.media.MediaFormat
import android.os.Build
import android.os.SystemClock
import android.view.Surface
import app.ft.core.DiagLog
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

object AaVideoSink {
    private val _surface = MutableStateFlow<Surface?>(null)
    val surface: StateFlow<Surface?> = _surface
    private val _frame = MutableStateFlow<Pair<Int, Int>?>(null)
    val frame: StateFlow<Pair<Int, Int>?> = _frame
    private val textureUpdates = AtomicLong(0L)
    @Volatile var lastTextureUpdateAtMs = 0L
        private set
    var width = 0
    var height = 0

    fun attach(surface: Surface, width: Int, height: Int) {
        this.width = width
        this.height = height
        _surface.value = surface
    }

    fun detach(surface: Surface?) {
        if (surface != null) _surface.compareAndSet(surface, null)
    }

    fun frameSize(width: Int, height: Int) {
        _frame.value = width to height
    }

    fun textureUpdated() {
        textureUpdates.incrementAndGet()
        lastTextureUpdateAtMs = SystemClock.elapsedRealtime()
    }

    fun textureUpdateCount(): Long = textureUpdates.get()
}

class AaVideoDecoder(private val width: Int, private val height: Int) {
    private val tag = "AaDecoder"
    private val lock = Any()
    private val running = AtomicBoolean(false)
    private val _frames = MutableStateFlow(0L)
    val frames: StateFlow<Long> = _frames

    @Volatile private var codec: MediaCodec? = null
    @Volatile private var config: ByteArray? = null
    @Volatile private var drain: Thread? = null
    @Volatile private var surface: Surface? = null
    @Volatile private var generation = 0L
    @Volatile private var missed = 0
    private var nextStartAttemptAt = 0L
    private var startFailures = 0
    private val queuedInputs = AtomicLong(0L)
    private val inputStarved = AtomicLong(0L)
    private val renderedOutputs = AtomicLong(0L)
    private val emptyOutputPolls = AtomicLong(0L)
    @Volatile private var lastInputAtMs = 0L
    @Volatile private var lastOutputAtMs = 0L
    @Volatile var askForAFreshPicture: (() -> Unit)? = null

    init {
        if (mediaTek) {
            DiagLog.i(tag, "this phone has a MediaTek chip (${Build.HARDWARE}), so FT makes a fresh decoder whenever the car screen changes")
        }
    }

    fun setSurface(s: Surface?) {
        synchronized(lock) {
            if (s === surface) return
            val had = surface != null
            surface = s
            if (s != null) {
                DiagLog.i(tag, "car screen surface attached valid=${s.isValid} id=${System.identityHashCode(s)}")
            } else if (had) {
                DiagLog.i(tag, "Android Auto's picture has nowhere to be drawn")
            }
            if (s == null) {
                stopLocked()
                return
            }
            if (!s.isValid) {
                DiagLog.w(tag, "the new car screen surface is not valid, waiting for the next one")
                surface = null
                stopLocked()
                return
            }
            val c = codec
            if (running.get() && c != null) {
                if (!mediaTek) {
                    val moved = runCatching { c.setOutputSurface(s) }
                    if (moved.isSuccess) {
                        DiagLog.i(tag, "decoder moved onto the new car screen surface")
                        return
                    }
                    DiagLog.w(tag, "the decoder would not move to the new car screen (${moved.exceptionOrNull()?.javaClass?.simpleName}), making a fresh one")
                }
                stopLocked()
                missed++
            }
            // Wait for the next IDR on the new surface, as Headswap does.
        }
    }

    fun onConfig(csd: ByteArray) {
        val copy = csd.copyOf()
        synchronized(lock) {
            if (config?.contentEquals(copy) == true) return
            config = copy
            // Match Headswap: wait for an IDR before starting/restarting the decoder.
            stopLocked()
        }
    }

    fun onFrame(data: ByteArray, ptsUs: Long) {
        synchronized(lock) {
            if (surface?.isValid != true) {
                if (surface != null) {
                    DiagLog.w(tag, "the car screen surface is no longer valid, stopping the decoder")
                    surface = null
                    stopLocked()
                }
                return
            }
            if (!running.get()) {
                if (surface == null || config == null || !isKeyFrame(data)) {
                    missed++
                    return
                }
                startLocked()
                if (!running.get()) {
                    missed++
                    return
                }
            }
            feedLocked(data, false, ptsUs)
        }
    }

    private fun startLocked() {
        val s = surface ?: return
        val csd = config ?: return
        if (!s.isValid) {
            DiagLog.w(tag, "not starting the decoder, the car screen surface is not valid")
            return
        }
        val now = SystemClock.elapsedRealtime()
        if (now < nextStartAttemptAt) return
        if (!running.compareAndSet(false, true)) return
        nextStartAttemptAt = now + START_RETRY_DELAY_MS
        val mine = generation + 1
        generation = mine
        var made: MediaCodec? = null
        try {
            val format = MediaFormat.createVideoFormat(MediaFormat.MIMETYPE_VIDEO_AVC, width, height)
            format.setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, 1 shl 20)
            format.setInteger(MediaFormat.KEY_LOW_LATENCY, 1)
            val c = MediaCodec.createDecoderByType(MediaFormat.MIMETYPE_VIDEO_AVC)
            made = c
            c.configure(format, s, null, 0)
            c.start()
            codec = c
            // Headswap queues SPS/PPS as a CODEC_CONFIG input buffer after start.
            val configSlot = c.dequeueInputBuffer(100_000)
            if (configSlot < 0) throw IllegalStateException("no decoder input buffer for codec config")
            val configBuffer = c.getInputBuffer(configSlot)
                ?: throw IllegalStateException("decoder returned a null codec-config input buffer")
            configBuffer.clear()
            if (csd.size > configBuffer.remaining()) throw IllegalArgumentException("codec config does not fit decoder input buffer")
            configBuffer.put(csd)
            c.queueInputBuffer(configSlot, 0, csd.size, 0L, MediaCodec.BUFFER_FLAG_CODEC_CONFIG)
            made = null
            AaVideoSink.frameSize(width, height)
            lastInputAtMs = SystemClock.elapsedRealtime()
            lastOutputAtMs = 0L
            drain = Thread({ drainLoop(c, mine) }, "FT-AaDrain-$mine").also { it.start() }
            startFailures = 0
            nextStartAttemptAt = 0L
            DiagLog.i(tag, "decoder started ${width}x$height on ${runCatching { c.name }.getOrNull()} generation=$mine surface=${System.identityHashCode(s)}")
            if (missed > 0) {
                DiagLog.i(tag, "skipped $missed pictures while waiting for a keyframe or a usable decoder")
                missed = 0
            }
        } catch (t: Throwable) {
            running.set(false)
            codec = null
            startFailures++
            nextStartAttemptAt = SystemClock.elapsedRealtime() + START_RETRY_DELAY_MS
            runCatching { made?.release() }
            DiagLog.e(tag, "decoder start failed (attempt $startFailures); retrying after ${START_RETRY_DELAY_MS}ms", t)
        }
    }

    private fun feedLocked(data: ByteArray, isConfig: Boolean, ptsUs: Long = 0L) {
        val c = codec ?: return
        var slot = -1
        var given = false
        try {
            // Keep the AA reader responsive; the drain thread must be free to release output buffers.
            slot = c.dequeueInputBuffer(INPUT_TIMEOUT_US)
            if (slot < 0) {
                inputStarved.incrementAndGet()
                return
            }
            val buf = c.getInputBuffer(slot)
            if (buf == null || data.size > buf.remaining()) {
                if (buf != null) DiagLog.w(tag, "a ${data.size} byte picture did not fit the decoder, skipping it")
                c.queueInputBuffer(slot, 0, 0, ptsUs, 0)
                given = true
                return
            }
            buf.clear()
            buf.put(data)
            c.queueInputBuffer(slot, 0, data.size, ptsUs, if (isConfig) MediaCodec.BUFFER_FLAG_CODEC_CONFIG else 0)
            given = true
            if (!isConfig) {
                val count = queuedInputs.incrementAndGet()
                lastInputAtMs = SystemClock.elapsedRealtime()
                if (count % 300L == 0L) {
                    DiagLog.i(tag, "decoder input checkpoint: queued=$count output=${renderedOutputs.get()} inputStarved=${inputStarved.get()}")
                }
            }
        } catch (t: Throwable) {
            DiagLog.e(tag, "feed failed", t)
            if (slot >= 0 && !given) runCatching { c.queueInputBuffer(slot, 0, 0, ptsUs, 0) }
        }
    }

    private fun drainLoop(c: MediaCodec, mine: Long) {
        val info = MediaCodec.BufferInfo()
        var lastStatsAt = SystemClock.elapsedRealtime()
        while (running.get() && generation == mine) {
            try {
                val idx = c.dequeueOutputBuffer(info, 20_000)
                if (idx >= 0) {
                    val current = synchronized(lock) { running.get() && generation == mine && codec === c }
                    if (!current) {
                        runCatching { c.releaseOutputBuffer(idx, false) }
                        return
                    }
                    // Do not hold the decoder lock while releasing to Surface: input queuing must not
                    // block the output thread, otherwise the codec can run out of output buffers.
                    c.releaseOutputBuffer(idx, true)
                    renderedOutputs.incrementAndGet()
                    lastOutputAtMs = SystemClock.elapsedRealtime()
                    _frames.value = _frames.value + 1
                } else if (idx == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) {
                    val current = synchronized(lock) { running.get() && generation == mine && codec === c }
                    if (current) shown(c.outputFormat)
                } else if (idx == MediaCodec.INFO_TRY_AGAIN_LATER) {
                    emptyOutputPolls.incrementAndGet()
                }

                val now = SystemClock.elapsedRealtime()
                if (now - lastStatsAt >= DIAGNOSTIC_INTERVAL_MS) {
                    logFlowStats(c, mine, now)
                    lastStatsAt = now
                }
            } catch (t: Throwable) {
                if (!running.get() || generation != mine) return
                DiagLog.e(tag, "drain failed generation=$mine", t)
                gaveUp(c, mine)
                return
            }
        }
    }

    private fun logFlowStats(c: MediaCodec, mine: Long, now: Long) {
        val lastTexture = AaVideoSink.lastTextureUpdateAtMs
        val textureAge = if (lastTexture == 0L) -1L else now - lastTexture
        val inputAge = if (lastInputAtMs == 0L) -1L else now - lastInputAtMs
        val outputAge = if (lastOutputAtMs == 0L) -1L else now - lastOutputAtMs
        DiagLog.i(
            tag,
            "video flow gen=$mine codec=${runCatching { c.name }.getOrNull()} " +
                "in=${queuedInputs.get()} out=${renderedOutputs.get()} starved=${inputStarved.get()} " +
                "emptyPolls=${emptyOutputPolls.get()} surfaceValid=${surface?.isValid == true} " +
                "textureUpdates=${AaVideoSink.textureUpdateCount()} textureAgeMs=$textureAge " +
                "inputAgeMs=$inputAge outputAgeMs=$outputAge"
        )
    }

    private fun gaveUp(dead: MediaCodec, mine: Long) {
        synchronized(lock) {
            if (codec !== dead || generation != mine) return
            running.set(false)
            codec = null
            nextStartAttemptAt = SystemClock.elapsedRealtime() + START_RETRY_DELAY_MS
            if (drain === Thread.currentThread()) drain = null
            runCatching { dead.stop() }
            runCatching { dead.release() }
            DiagLog.w(tag, "the decoder stopped; FT will retry after ${START_RETRY_DELAY_MS}ms")
        }
    }

    // H.264 Annex-B IDR (NAL type 5), matching Headswap's keyframe gate.
    private fun isKeyFrame(data: ByteArray): Boolean {
        var i = 0
        while (i + 3 < data.size) {
            if (data[i] == 0.toByte() && data[i + 1] == 0.toByte() && data[i + 2] == 1.toByte()) {
                val type = data[i + 3].toInt() and 0x1F
                if (type == 5 || type == 7) return true
                i += 3
            } else if (i + 4 < data.size && data[i] == 0.toByte() && data[i + 1] == 0.toByte() &&
                data[i + 2] == 0.toByte() && data[i + 3] == 1.toByte()) {
                val type = data[i + 4].toInt() and 0x1F
                if (type == 5 || type == 7) return true
                i += 4
            } else i++
        }
        return false
    }

    private fun shown(f: MediaFormat) {
        val w = if (f.containsKey("crop-right") && f.containsKey("crop-left")) f.getInteger("crop-right") - f.getInteger("crop-left") + 1 else f.getInteger(MediaFormat.KEY_WIDTH)
        val h = if (f.containsKey("crop-bottom") && f.containsKey("crop-top")) f.getInteger("crop-bottom") - f.getInteger("crop-top") + 1 else f.getInteger(MediaFormat.KEY_HEIGHT)
        if (w <= 0 || h <= 0) return
        DiagLog.i(tag, "Android Auto decoder output format=${w}x$h, requested=${width}x$height")
        AaVideoSink.frameSize(w, h)
    }

    private fun stopLocked() {
        val old = codec
        generation++
        if (!running.getAndSet(false) && old == null) return
        codec = null
        drain = null
        runCatching { old?.stop() }
        runCatching { old?.release() }
    }

    fun stop() {
        synchronized(lock) { stopLocked() }
    }

    fun reset() {
        synchronized(lock) {
            stopLocked()
            config = null
            askForAFreshPicture = null
        }
    }

    companion object {
        private const val START_RETRY_DELAY_MS = 750L
        private const val INPUT_TIMEOUT_US = 5_000L
        private const val DIAGNOSTIC_INTERVAL_MS = 5_000L

        val mediaTek: Boolean by lazy {
            val hardware = Build.HARDWARE.orEmpty().lowercase()
            val board = Build.BOARD.orEmpty().lowercase()
            val maker = Build.MANUFACTURER.orEmpty().lowercase()
            hardware.startsWith("mt") || hardware.contains("mtk") || hardware.contains("mediatek") ||
                board.startsWith("mt") || maker.contains("mediatek")
        }
    }
}
