package app.ft.aa

import android.content.Context
import app.ft.core.Bytes
import app.ft.core.CarAudioBus
import app.ft.core.DiagLog
import app.ft.core.Prefs
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.delay
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray
import java.util.concurrent.atomic.AtomicIntegerArray
import java.io.InputStream
import java.io.OutputStream

private val OFF_MAIN = java.util.concurrent.Executors.newSingleThreadExecutor { r -> Thread(r, "ft-aa-main-sends").apply { isDaemon = true } }

class AaSession(
    context: Context,
    private val input: InputStream,
    private val output: OutputStream,
    private val transportState: () -> String,
    private val prefs: Prefs,
    private val scope: CoroutineScope,
    private val decoder: AaVideoDecoder,
    private val onClosed: (String) -> Unit
) {
    enum class Phase { CONNECTED, VERSIONED, HANDSHAKING, AUTHENTICATED, DISCOVERED, STREAMING }

    private val tag = "AA"
    private val crypto = AaCrypto(context)
    private val assembler = AaFraming.Assembler { crypto.decrypt(it) }
    private val outLock = Any()
    private var job: Job? = null
    private val lifecycle = AaSessionLifecycle(
        interrupt = {
            runCatching { input.close() }
            runCatching { output.close() }
            job?.cancel()
            transportMonitorJob?.cancel()
            keepaliveJob?.cancel()
            videoMonitor?.cancel()
        },
        cleanup = { reason ->
            runCatching { mic.stop() }
            if (micWanted) {
                micWanted = false
                runCatching { AaHeadUnitService.onCarMicWanted?.invoke(false) }
            }
            runCatching { CarAudioBus.end(AaProtocol.CH_SPEECH_AUDIO) }
            runCatching { CarAudioBus.end(AaProtocol.CH_SYSTEM_AUDIO) }
            runCatching { synchronized(navigation) { AaHeadUnitService.onNavigation?.invoke(navigation.reset()) } }
            try { decoder.reset() } finally { onClosed(reason) }
        }
    )
    private var videoSession = 0
    private val audioSessions = HashMap<Int, Int>()
    private val audioMeasured = HashSet<Int>()
    @Volatile private var mediaFrames = 0L
    private var videoMonitor: Job? = null
    private var transportMonitorJob: Job? = null
    private var keepaliveJob: Job? = null
    private val readCalls = AtomicLong(0L)
    private val outboundFrames = AtomicLong(0L)
    private val lastReadStartedAtMs = AtomicLong(0L)
    private val lastReadCompletedAtMs = AtomicLong(0L)
    private val lastWriteAtMs = AtomicLong(0L)
    private val lastPingToken = AtomicLong(0L)
    private val lastPingResponseToken = AtomicLong(0L)
    private val pingResponses = AtomicLong(0L)
    private val missedPingRounds = AtomicLong(0L)
    // Counts raw channel-3 frames before decryption/assembly/dispatch. This distinguishes
    // a phone-side video stall from a framing, crypto, or message-assembly problem.
    private val rawVideoFrames = AtomicLong(0L)
    private val rawFramesByChannel = AtomicLongArray(CHANNEL_COUNTERS)
    private val rawBytesByChannel = AtomicLongArray(CHANNEL_COUNTERS)
    private val dispatchedMessagesByChannel = AtomicLongArray(CHANNEL_COUNTERS)
    private val lastDispatchedIdByChannel = AtomicIntegerArray(CHANNEL_COUNTERS)
    private val lastDispatchedAtByChannel = AtomicLongArray(CHANNEL_COUNTERS)
    private val totalRawFrames = AtomicLong(0L)
    @Volatile private var lastAnyRawFrameAtMs = 0L
    private val outboundVideoAcks = AtomicLong(0L)
    private val rawVideoBytes = AtomicLong(0L)
    @Volatile private var lastRawVideoFrameAtMs = 0L
    private val videoPackets = AtomicLong(0L)
    private val videoPayloadBytes = AtomicLong(0L)
    private val videoMalformedPackets = AtomicLong(0L)
    private val videoRecoveryRequests = AtomicLong(0L)
    @Volatile private var lastVideoRecoveryRequestAtMs = 0L
    @Volatile private var lastVideoPacketAtMs = 0L
    @Volatile private var lastVideoPacketBytes = 0
    private val _phase = MutableStateFlow(Phase.CONNECTED)
    val phase: StateFlow<Phase> = _phase
    @Volatile var hasStreamed = false
        private set
    @Volatile var remoteBye = false
        private set
    private val _deviceName = MutableStateFlow("")
    val deviceName: StateFlow<String> = _deviceName
    private val openChannels = java.util.Collections.synchronizedSet(HashSet<Int>())
    private val navigation = AaNavigation()
    private val mic = AaMicrophone { pcm -> sendMicrophone(pcm) }
    @Volatile private var micWanted = false
    @Volatile private var carMicAt = 0L

    fun start() {
        if (!lifecycle.start()) return
        job = scope.launch(Dispatchers.IO) {
            var reason = "reader stopped"
            try {
                sendPlain(AaProtocol.CH_CONTROL, AaProtocol.VERSION_REQUEST, AaMessages.versionRequest(), false)
                DiagLog.i(tag, "version request sent")
                startTransportMonitor()
                while (isActive && lifecycle.running) {
                    lastReadStartedAtMs.set(android.os.SystemClock.elapsedRealtime())
                    val frame = AaFraming.readFrame(input)
                    val frameNow = android.os.SystemClock.elapsedRealtime()
                    readCalls.incrementAndGet()
                    lastReadCompletedAtMs.set(frameNow)
                    totalRawFrames.incrementAndGet()
                    lastAnyRawFrameAtMs = frameNow
                    if (frame.channel in 0 until CHANNEL_COUNTERS) {
                        rawFramesByChannel.incrementAndGet(frame.channel)
                        rawBytesByChannel.addAndGet(frame.channel, frame.payload.size.toLong())
                    }
                    if (frame.channel == AaProtocol.CH_VIDEO) {
                        val rawCount = rawVideoFrames.incrementAndGet()
                        rawVideoBytes.addAndGet(frame.payload.size.toLong())
                        lastRawVideoFrameAtMs = android.os.SystemClock.elapsedRealtime()
                        if (rawCount <= 8L) {
                            DiagLog.i(tag, "raw video socket frame #$rawCount flags=0x${Integer.toHexString(frame.flags)} bytes=${frame.payload.size} encrypted=${frame.encrypted} type=${frame.frameType}")
                        }
                    }
                    lifecycle.dispatch {
                        assembler.feed(frame)?.let { message ->
                            if (message.channel in 0 until CHANNEL_COUNTERS) {
                                dispatchedMessagesByChannel.incrementAndGet(message.channel)
                                lastDispatchedIdByChannel.set(message.channel, message.id)
                                lastDispatchedAtByChannel.set(message.channel, android.os.SystemClock.elapsedRealtime())
                            }
                            dispatch(message)
                        }
                    }
                }
            } catch (t: Throwable) {
                reason = "read ${t.javaClass.simpleName}: ${t.message}"
                if (lifecycle.running) DiagLog.w(tag, "session ended: $reason")
            } finally {
                close(reason)
            }
        }
    }

    fun close(reason: String) {
        lifecycle.close(reason)
    }

    private fun sendPlain(channel: Int, id: Int, body: ByteArray, control: Boolean) = send(channel, id, body, false, control)
    private fun sendEnc(channel: Int, id: Int, body: ByteArray, control: Boolean = false) = send(channel, id, body, true, control)

    private fun send(channel: Int, id: Int, body: ByteArray, encrypted: Boolean, control: Boolean) {
        if (!lifecycle.running) return
        if (android.os.Looper.myLooper() == android.os.Looper.getMainLooper()) {
            OFF_MAIN.execute { send(channel, id, body, encrypted, control) }
            return
        }
        val payload = ByteArray(2 + body.size)
        Bytes.putU16(id, payload, 0)
        System.arraycopy(body, 0, payload, 2, body.size)
        val failure = synchronized(outLock) {
            if (!lifecycle.running) return
            try {
                // CONTROL is a per-message flag, not something implied by channel 0.
                // VERSION/TLS/auth and other channel-0 messages use BULK flags; control
                // messages carried on service channels explicitly opt in at the call site.
                val frames = AaFraming.encode(channel, payload, encrypted, control) { if (encrypted) crypto.encrypt(it) else it }
                if (channel == AaProtocol.CH_CONTROL && outboundFrames.get() < 40L) {
                    val flags = frames.firstOrNull()?.getOrNull(1)?.toInt()?.and(0xff) ?: -1
                    DiagLog.d(tag, "TX CONTROL ${AaProtocol.controlName(id)} encrypted=$encrypted flags=0x${Integer.toHexString(flags)} bytes=${body.size}")
                }
                if (channel == AaProtocol.CH_VIDEO && id == AaProtocol.AV_MEDIA_ACK) {
                    val ackCount = outboundVideoAcks.incrementAndGet()
                    if (ackCount <= 5L || ackCount % 100L == 0L) {
                        DiagLog.d(tag, "video media ACK sent #$ackCount")
                    }
                }
                frames.forEach { output.write(it) }
                output.flush()
                outboundFrames.addAndGet(frames.size.toLong())
                lastWriteAtMs.set(android.os.SystemClock.elapsedRealtime())
                null
            } catch (t: Throwable) {
                t
            }
        }
        if (failure != null) {
            if (lifecycle.running) DiagLog.e(tag, "write failed", failure)
            close("write ${failure.javaClass.simpleName}: ${failure.message}")
        }
    }

    private fun dispatch(m: AaFraming.Message) {
        val id = m.id
        val body = m.body
        if (m.channel == AaProtocol.CH_CONTROL || m.control) {
            handleControl(m.channel, id, body)
            return
        }
        when (m.channel) {
            AaProtocol.CH_VIDEO -> handleVideo(id, body)
            AaProtocol.CH_INPUT -> handleInput(id, body)
            AaProtocol.CH_SENSOR -> handleSensor(id, body)
            AaProtocol.CH_MEDIA_AUDIO, AaProtocol.CH_SPEECH_AUDIO, AaProtocol.CH_SYSTEM_AUDIO -> handleAudioSink(m.channel, id, body)
            AaProtocol.CH_AV_INPUT -> handleAvInput(id, body)
            AaProtocol.CH_NAVIGATION -> synchronized(navigation) {
                if (lifecycle.running) navigation.accept(id, body)?.let {
                    AaHeadUnitService.onNavigation?.invoke(it)
                }
            }
            else -> DiagLog.d(tag, "${AaProtocol.channelName(m.channel)} msg 0x${Integer.toHexString(id)} len=${body.size}")
        }
    }

    private fun handleControl(channel: Int, id: Int, body: ByteArray) {
        if (id != AaProtocol.PING_REQUEST && id != AaProtocol.SSL_HANDSHAKE) DiagLog.rx(tag, "${AaProtocol.channelName(channel)} ${AaProtocol.controlName(id)}", body)
        when (id) {
            AaProtocol.VERSION_RESPONSE -> {
                val v = AaMessages.parseVersionResponse(body)
                DiagLog.i(tag, "phone protocol ${v.major}.${v.minor} status=${v.status}")
                _phase.value = Phase.VERSIONED
                val hello = crypto.handshakeStep(null)
                if (hello.isNotEmpty()) sendPlain(AaProtocol.CH_CONTROL, AaProtocol.SSL_HANDSHAKE, hello, false)
                _phase.value = Phase.HANDSHAKING
            }
            AaProtocol.SSL_HANDSHAKE -> {
                val out = crypto.handshakeStep(body)
                if (out.isNotEmpty()) sendPlain(AaProtocol.CH_CONTROL, AaProtocol.SSL_HANDSHAKE, out, false)
                if (crypto.handshakeDone) {
                    sendPlain(AaProtocol.CH_CONTROL, AaProtocol.AUTH_COMPLETE, AaMessages.authComplete(), false)
                    _phase.value = Phase.AUTHENTICATED
                    startKeepalive()
                    DiagLog.i(tag, "TLS handshake complete, auth complete sent")
                }
            }
            AaProtocol.SERVICE_DISCOVERY_REQUEST -> {
                _deviceName.value = AaMessages.deviceName(body)
                DiagLog.i(tag, "service discovery from '${_deviceName.value}'")
                DiagLog.i(tag, "video settings ${prefs.aaWidth}x${prefs.aaHeight}@${prefs.aaFps} fps")
                sendEnc(
                    AaProtocol.CH_CONTROL, AaProtocol.SERVICE_DISCOVERY_RESPONSE,
                    AaMessages.serviceDiscoveryResponse(prefs.aaWidth, prefs.aaHeight, prefs.aaFps, prefs.aaDensity, true, prefs.carName, prefs.aaControlsLeft)
                )
                _phase.value = Phase.DISCOVERED
            }
            AaProtocol.CHANNEL_OPEN_REQUEST -> {
                // Android Auto may carry this control message on the service channel being opened.
                // The request body is priority (field 1) + channel ID (field 2), not service_type.
                val priority = AaMessages.channelOpenPriority(body)
                val targetChannel = AaMessages.channelOpenChannelId(body)
                if (targetChannel !in 1 until CHANNEL_COUNTERS) {
                    DiagLog.e(tag, "invalid CHANNEL_OPEN_REQUEST priority=$priority frameChannel=$channel channel=$targetChannel bodyBytes=${body.size}")
                    return
                }
                openChannels += targetChannel
                DiagLog.i(tag, "channel open request priority=$priority frameChannel=$channel targetChannel=$targetChannel (${AaProtocol.channelName(targetChannel)})")
                // Android Auto carries CHANNEL_OPEN_REQUEST on the service channel;
                // acknowledge on that same incoming channel with the CONTROL flag.
                sendEnc(channel, AaProtocol.CHANNEL_OPEN_RESPONSE, AaMessages.channelOpenResponse(), true)
                if (targetChannel == AaProtocol.CH_NAVIGATION) {
                    sendEnc(targetChannel, AaProtocol.NAV_START, ByteArray(0))
                    DiagLog.i(tag, "navigation status started on channel=$targetChannel")
                }
            }
            AaProtocol.PING_REQUEST -> {
                DiagLog.d(tag, "phone ping request received")
                sendEnc(AaProtocol.CH_CONTROL, AaProtocol.PING_RESPONSE, AaMessages.pingResponse(AaMessages.pingTimestamp(body)))
            }
            AaProtocol.PING_RESPONSE -> {
                val echoed = AaMessages.pingTimestamp(body)
                val now = android.os.SystemClock.elapsedRealtime()
                val sent = lastPingToken.get()
                if (echoed == sent && sent != 0L) {
                    lastPingResponseToken.set(echoed)
                    val count = pingResponses.incrementAndGet()
                    DiagLog.i(tag, "phone ping response #$count rttMs=${(now - sent).coerceAtLeast(0L)}")
                } else {
                    DiagLog.w(tag, "unexpected ping response echoed=$echoed lastSent=$sent")
                }
            }
            AaProtocol.AUDIO_FOCUS_REQUEST -> {
                val req = AaMessages.audioFocusRequestType(body)
                val state = if (req == AaProtocol.AUDIO_REQUEST_RELEASE) AaProtocol.AUDIO_FOCUS_LOSS else AaProtocol.AUDIO_FOCUS_GAIN
                sendEnc(AaProtocol.CH_CONTROL, AaProtocol.AUDIO_FOCUS_NOTIFICATION, AaMessages.audioFocusNotification(state))
            }
            AaProtocol.NAV_FOCUS_REQUEST -> sendEnc(AaProtocol.CH_CONTROL, AaProtocol.NAV_FOCUS_NOTIFICATION, AaMessages.navFocusNotification(AaProtocol.NAV_FOCUS_PROJECTED))
            AaProtocol.BYEBYE_REQUEST -> {
                remoteBye = true
                DiagLog.i(tag, "phone bye-bye reason=${AaMessages.byeByeReason(body)}")
                sendEnc(AaProtocol.CH_CONTROL, AaProtocol.BYEBYE_RESPONSE, AaMessages.byeByeResponse())
                close("bye-bye")
            }
            AaProtocol.CHANNEL_CLOSE_NOTIFICATION -> {
                val targetChannel = AaMessages.channelOpenChannelId(body).takeIf { it in 1 until CHANNEL_COUNTERS } ?: channel
                openChannels -= targetChannel
                if (targetChannel == AaProtocol.CH_NAVIGATION) synchronized(navigation) {
                    AaHeadUnitService.onNavigation?.invoke(navigation.reset())
                }
                DiagLog.i(tag, "channel close notification target=$targetChannel (${AaProtocol.channelName(targetChannel)})")
            }
            AaProtocol.VOICE_SESSION_NOTIFICATION -> Unit
            else -> DiagLog.w(tag, "unhandled control ${AaProtocol.controlName(id)} on ${AaProtocol.channelName(channel)}")
        }
    }

    fun askForAFreshPicture() {
        if (_phase.value != Phase.STREAMING) return
        runCatching {
            sendEnc(AaProtocol.CH_VIDEO, AaProtocol.VIDEO_FOCUS_INDICATION, AaMessages.videoFocusIndication(AaProtocol.VIDEO_FOCUS_PROJECTED, true))
        }.onFailure { DiagLog.w(tag, "could not ask Android Auto for a fresh picture: ${it.message}") }
    }

    private fun handleVideo(id: Int, body: ByteArray) {
        if (id == AaProtocol.AV_MEDIA_INDICATION || id == AaProtocol.AV_MEDIA_WITH_TIMESTAMP) {
            videoPackets.incrementAndGet()
            videoPayloadBytes.addAndGet(body.size.toLong())
            val packetNow = android.os.SystemClock.elapsedRealtime()
            val previousPacketAt = lastVideoPacketAtMs
            if (previousPacketAt != 0L && packetNow - previousPacketAt >= VIDEO_STALL_TIMEOUT_MS) {
                videoRecoveryRequests.set(0L)
                lastVideoRecoveryRequestAtMs = 0L
                DiagLog.i(tag, "video packets resumed after ${packetNow - previousPacketAt}ms")
            }
            lastVideoPacketAtMs = packetNow
            lastVideoPacketBytes = body.size
            if (id == AaProtocol.AV_MEDIA_WITH_TIMESTAMP && body.size <= 8) videoMalformedPackets.incrementAndGet()
        }
        when (id) {
            AaProtocol.AV_SETUP_REQUEST -> {
                val configIndex = AaMessages.setupConfigIndex(body)
                DiagLog.i(tag, "video setup requested configIndex=$configIndex; echoing it in AV_SETUP_RESPONSE")
                sendEnc(AaProtocol.CH_VIDEO, AaProtocol.AV_SETUP_RESPONSE, AaMessages.avSetupResponse(configIndex = configIndex))
                // This is a proactive focus grant, not a reply to a phone focus request.
                sendEnc(AaProtocol.CH_VIDEO, AaProtocol.VIDEO_FOCUS_INDICATION, AaMessages.videoFocusIndication(AaProtocol.VIDEO_FOCUS_PROJECTED, true))
            }
            AaProtocol.AV_START_INDICATION -> {
                DiagLog.w(tag, "VIDEO EVENT: AV_START_INDICATION bodyBytes=${body.size}")
                hasStreamed = true
                videoSession = AaMessages.startSession(body)
                mediaFrames = 0
                rawVideoFrames.set(0L)
                rawVideoBytes.set(0L)
                lastRawVideoFrameAtMs = 0L
                videoPackets.set(0L)
                videoPayloadBytes.set(0L)
                videoMalformedPackets.set(0L)
                videoRecoveryRequests.set(0L)
                lastVideoRecoveryRequestAtMs = 0L
                lastVideoPacketAtMs = 0L
                lastVideoPacketBytes = 0
                videoMonitor?.cancel()
                _phase.value = Phase.STREAMING
                val monitoredSession = videoSession
                videoMonitor = scope.launch {
                    while (isActive && _phase.value == Phase.STREAMING && videoSession == monitoredSession) {
                        delay(5_000)
                        val now = android.os.SystemClock.elapsedRealtime()
                        val age = if (lastVideoPacketAtMs == 0L) -1L else now - lastVideoPacketAtMs
                        val rawAge = if (lastRawVideoFrameAtMs == 0L) -1L else now - lastRawVideoFrameAtMs
                        val rawCount = rawVideoFrames.get()
                        val packetCount = videoPackets.get()
                        val anyRawAge = if (lastAnyRawFrameAtMs == 0L) -1L else now - lastAnyRawFrameAtMs
                        val rawChannels = (0 until CHANNEL_COUNTERS).mapNotNull { ch ->
                            val count = rawFramesByChannel.get(ch)
                            if (count == 0L) null else "$ch:$count/${rawBytesByChannel.get(ch)}"
                        }.joinToString(",")
                        val dispatchedChannels = (0 until CHANNEL_COUNTERS).mapNotNull { ch ->
                            val count = dispatchedMessagesByChannel.get(ch)
                            if (count == 0L) null else "$ch:$count(last=0x${Integer.toHexString(lastDispatchedIdByChannel.get(ch))},age=${if (lastDispatchedAtByChannel.get(ch) == 0L) -1L else now - lastDispatchedAtByChannel.get(ch)}ms)"
                        }.joinToString(",")
                        if (packetCount > 0L && age >= VIDEO_STALL_TIMEOUT_MS && videoRecoveryRequests.get() < MAX_VIDEO_RECOVERY_REQUESTS && now - lastVideoRecoveryRequestAtMs >= VIDEO_RECOVERY_INTERVAL_MS) {
                            val attempt = videoRecoveryRequests.incrementAndGet()
                            lastVideoRecoveryRequestAtMs = now
                            DiagLog.w(tag, "video stream stalled for ${age}ms; requesting a fresh picture (attempt $attempt/$MAX_VIDEO_RECOVERY_REQUESTS)")
                            askForAFreshPicture()
                        }
                        DiagLog.i(tag, "transport session=" + monitoredSession +
                            " phase=" + _phase.value +
                            " totalRawFrames=" + totalRawFrames.get() +
                            " anyRawAgeMs=" + anyRawAge +
                            " rawByChannel=[" + rawChannels + "]" +
                            " dispatchedByChannel=[" + dispatchedChannels + "]" +
                            " videoRawFrames=" + rawCount +
                            " videoRawBytes=" + rawVideoBytes.get() +
                            " videoRawAgeMs=" + rawAge +
                            " videoPackets=" + packetCount +
                            " videoPayloadBytes=" + videoPayloadBytes.get() +
                            " videoFramesParsed=" + mediaFrames +
                            " videoMalformed=" + videoMalformedPackets.get() +
                            " videoLastPacketAgeMs=" + age +
                            " videoLastPacketBytes=" + lastVideoPacketBytes +
                            " videoRecoveryRequests=" + videoRecoveryRequests.get() +
                            " videoAcksSent=" + outboundVideoAcks.get() +
                            " decoderRendered=" + decoder.frames.value)
                    }
                }
                decoder.askForAFreshPicture = { scope.launch { askForAFreshPicture() } }
                DiagLog.i(tag, "video start session=$videoSession")
                sendEnc(AaProtocol.CH_VIDEO, AaProtocol.VIDEO_FOCUS_INDICATION, AaMessages.videoFocusIndication(AaProtocol.VIDEO_FOCUS_PROJECTED, true))
            }
            AaProtocol.AV_STOP_INDICATION -> {
                DiagLog.w(tag, "VIDEO EVENT: AV_STOP_INDICATION bodyBytes=${body.size}")
                DiagLog.i(tag, "video stop")
                videoMonitor?.cancel()
                videoMonitor = null
                decoder.stop()
                _phase.value = Phase.DISCOVERED
            }
            AaProtocol.AV_MEDIA_INDICATION -> {
                DiagLog.i(tag, "video codec config ${body.size} bytes")
                decoder.onConfig(body)
                sendEnc(AaProtocol.CH_VIDEO, AaProtocol.AV_MEDIA_ACK, AaMessages.mediaAck(videoSession))
            }
            AaProtocol.AV_MEDIA_WITH_TIMESTAMP -> {
                if (body.size > 8) {
                    val ts = Bytes.u64(body, 0)
                    mediaFrames++
                    if (mediaFrames <= 3 || mediaFrames % 300 == 0L) DiagLog.d(tag, "video frame #$mediaFrames ${body.size - 8} bytes, ${decoder.frames.value} drawn on the car screen")
                    decoder.onFrame(body.copyOfRange(8, body.size), ts)
                }
                sendEnc(AaProtocol.CH_VIDEO, AaProtocol.AV_MEDIA_ACK, AaMessages.mediaAck(videoSession))
            }
            AaProtocol.VIDEO_FOCUS_REQUEST -> {
                val mode = AaMessages.videoFocusRequestMode(body)
                sendEnc(AaProtocol.CH_VIDEO, AaProtocol.VIDEO_FOCUS_INDICATION, AaMessages.videoFocusIndication(mode, false))
            }
            else -> DiagLog.w(tag, "unhandled video ${AaProtocol.avName(id)}")
        }
    }

    private fun handleInput(id: Int, body: ByteArray) {
        when (id) {
            AaProtocol.BINDING_REQUEST -> {
                DiagLog.i(tag, "input binding request")
                sendEnc(AaProtocol.CH_INPUT, AaProtocol.BINDING_RESPONSE, AaMessages.bindingResponse())
            }
            else -> DiagLog.d(tag, "input msg 0x${Integer.toHexString(id)}")
        }
    }

    private fun handleSensor(id: Int, body: ByteArray) {
        when (id) {
            AaProtocol.SENSOR_START_REQUEST -> {
                val type = AaMessages.sensorRequestType(body)
                sendEnc(AaProtocol.CH_SENSOR, AaProtocol.SENSOR_START_RESPONSE, AaMessages.sensorStartResponse())
                when (type) {
                    AaProtocol.SENSOR_DRIVING_STATUS -> sendEnc(AaProtocol.CH_SENSOR, AaProtocol.SENSOR_EVENT_INDICATION, AaMessages.sensorDrivingStatus(0))
                    AaProtocol.SENSOR_NIGHT_MODE -> sendEnc(AaProtocol.CH_SENSOR, AaProtocol.SENSOR_EVENT_INDICATION, AaMessages.sensorNightMode(false))
                    else -> Unit
                }
                DiagLog.i(tag, "sensor start type=$type")
            }
            else -> DiagLog.d(tag, "sensor msg 0x${Integer.toHexString(id)}")
        }
    }

    private fun handleAudioSink(channel: Int, id: Int, body: ByteArray) {
        when (id) {
            AaProtocol.AV_SETUP_REQUEST -> {
                val configIndex = AaMessages.setupConfigIndex(body)
                DiagLog.i(tag, "${AaProtocol.channelName(channel)} setup configIndex=$configIndex; echoing selected config")
                sendEnc(channel, AaProtocol.AV_SETUP_RESPONSE, AaMessages.avSetupResponse(configIndex = configIndex))
            }
            AaProtocol.AV_START_INDICATION -> {
                audioSessions[channel] = AaMessages.startSession(body)
                audioMeasured -= channel
                CarAudioBus.begin(channel)
                DiagLog.i(tag, "${AaProtocol.channelName(channel)} started")
            }
            AaProtocol.AV_STOP_INDICATION -> {
                CarAudioBus.end(channel)
                DiagLog.i(tag, "${AaProtocol.channelName(channel)} stopped")
            }
            AaProtocol.AV_MEDIA_WITH_TIMESTAMP, AaProtocol.AV_MEDIA_INDICATION -> {
                val pcm = if (id == AaProtocol.AV_MEDIA_WITH_TIMESTAMP && body.size > 8) body.copyOfRange(8, body.size) else body
                sendEnc(channel, AaProtocol.AV_MEDIA_ACK, AaMessages.mediaAck(audioSessions[channel] ?: 0))
                if (pcm.isNotEmpty()) {
                    val rate = if (channel == AaProtocol.CH_MEDIA_AUDIO) 48000 else 16000
                    val ch = if (channel == AaProtocol.CH_MEDIA_AUDIO) 2 else 1
                    if (audioMeasured.add(channel)) {
                        DiagLog.i(tag, "${AaProtocol.channelName(channel)} sends ${pcm.size} byte packets, ${pcm.size * 1000 / (rate * ch * 2)} ms each")
                    }
                    CarAudioBus.play(channel, pcm, rate, ch)
                }
            }
            else -> Unit
        }
    }

    private fun startMicrophone() {
        if (mic.active) return
        if (!AaHeadUnitService.micReady) {
            DiagLog.w(tag, "Android Auto asked to listen, but FT was started in the background so android will not give it the microphone; open FT and start Android Auto from there")
        }
        if (System.nanoTime() - carMicAt < 2_000_000_000L) {
            DiagLog.i(tag, "using the car's microphone for Android Auto")
            return
        }
        if (!mic.start()) DiagLog.w(tag, "Android Auto asked to listen but the microphone is not available")
    }

    fun feedCarMicrophone(pcm: ByteArray) {
        carMicAt = System.nanoTime()
        if (!micWanted) return
        if (mic.active) {
            DiagLog.i(tag, "the car is sending its own microphone, letting go of the phone's")
            mic.stop()
        }
        sendMicrophone(pcm)
    }

    private fun startTransportMonitor() {
        if (transportMonitorJob != null) return
        transportMonitorJob = scope.launch {
            while (isActive && lifecycle.running) {
                delay(5_000)
                val now = android.os.SystemClock.elapsedRealtime()
                val readStarted = lastReadStartedAtMs.get()
                val readCompleted = lastReadCompletedAtMs.get()
                val blockedFor = if (readStarted == 0L) -1L else now - readStarted
                val sinceRx = if (readCompleted == 0L) -1L else now - readCompleted
                val sinceTx = if (lastWriteAtMs.get() == 0L) -1L else now - lastWriteAtMs.get()
                val socket = runCatching { transportState() }.getOrElse { "stateError=${it.javaClass.simpleName}" }
                DiagLog.i(tag, "transport health phase=${_phase.value} readCalls=${readCalls.get()} readBlockedMs=$blockedFor sinceRxMs=$sinceRx sinceTxMs=$sinceTx outboundFrames=${outboundFrames.get()} pingResponses=${pingResponses.get()} missedPingRounds=${missedPingRounds.get()} openChannels=${synchronized(openChannels) { openChannels.toList().sorted() }} socket={$socket}")
            }
        }
    }

    private fun startKeepalive() {
        if (keepaliveJob != null) return
        keepaliveJob = scope.launch {
            while (isActive && lifecycle.running) {
                delay(15_000)
                if (!lifecycle.running || _phase.value < Phase.AUTHENTICATED) continue
                val previous = lastPingToken.get()
                if (previous != 0L && lastPingResponseToken.get() != previous) {
                    val missed = missedPingRounds.incrementAndGet()
                    DiagLog.w(tag, "keepalive ping unanswered round=$missed token=$previous; transport read may still be open")
                }
                val token = android.os.SystemClock.elapsedRealtime()
                lastPingToken.set(token)
                sendEnc(AaProtocol.CH_CONTROL, AaProtocol.PING_REQUEST, AaMessages.pingRequest(token))
                DiagLog.d(tag, "keepalive ping sent token=$token")
            }
        }
    }

    companion object {
        private const val VIDEO_STALL_TIMEOUT_MS = 5_000L
        private const val VIDEO_RECOVERY_INTERVAL_MS = 10_000L
        private const val MAX_VIDEO_RECOVERY_REQUESTS = 3L
        private const val CHANNEL_COUNTERS = 16
    }

    private fun sendMicrophone(pcm: ByteArray) {
        if (!lifecycle.running) return
        val payload = ByteArray(8 + pcm.size)
        Bytes.putU64(System.nanoTime() / 1000L, payload, 0)
        System.arraycopy(pcm, 0, payload, 8, pcm.size)
        sendEnc(AaProtocol.CH_AV_INPUT, AaProtocol.AV_MEDIA_WITH_TIMESTAMP, payload)
    }

    private fun handleAvInput(id: Int, body: ByteArray) {
        when (id) {
            AaProtocol.AV_SETUP_REQUEST -> {
                val configIndex = AaMessages.setupConfigIndex(body)
                sendEnc(AaProtocol.CH_AV_INPUT, AaProtocol.AV_SETUP_RESPONSE, AaMessages.avSetupResponse(configIndex = configIndex))
            }
            AaProtocol.AV_INPUT_OPEN_REQUEST -> {
                val wanted = AaMessages.avInputOpenWanted(body)
                sendEnc(AaProtocol.CH_AV_INPUT, AaProtocol.AV_INPUT_OPEN_RESPONSE, AaMessages.avInputOpenResponse(if (wanted) 0 else AaMessages.avInputOpenSession(body)))
                if (wanted != micWanted) DiagLog.i(tag, if (wanted) "Android Auto opened the microphone" else "Android Auto closed the microphone")
                micWanted = wanted
                runCatching { AaHeadUnitService.onCarMicWanted?.invoke(wanted) }
                if (wanted) startMicrophone() else mic.stop()
            }
            AaProtocol.AV_MEDIA_ACK -> Unit
            else -> Unit
        }
    }

    fun sendTouch(x: Int, y: Int, action: Int) {
        if (_phase.value < Phase.DISCOVERED || AaProtocol.CH_INPUT !in openChannels) return
        val aaAction = when (action) {
            0 -> AaProtocol.POINTER_DOWN
            1 -> AaProtocol.POINTER_UP
            else -> AaProtocol.POINTER_MOVED
        }
        sendEnc(AaProtocol.CH_INPUT, AaProtocol.INPUT_EVENT_INDICATION, AaMessages.inputTouch(System.nanoTime(), x, y, aaAction))
    }

    fun sendKey(keycode: Int) {
        if (_phase.value < Phase.DISCOVERED || AaProtocol.CH_INPUT !in openChannels) return
        sendEnc(AaProtocol.CH_INPUT, AaProtocol.INPUT_EVENT_INDICATION, AaMessages.inputKey(System.nanoTime(), keycode, true))
        sendEnc(AaProtocol.CH_INPUT, AaProtocol.INPUT_EVENT_INDICATION, AaMessages.inputKey(System.nanoTime(), keycode, false))
    }
}
