package net.aholbrook.kotlinleaderelection

import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.assertions.throwables.shouldNotThrowAny
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.shouldBe
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.models.V1Lease
import io.kubernetes.client.openapi.models.V1LeaseSpec
import io.kubernetes.client.openapi.models.V1ObjectMeta
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.time.ZoneOffset
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.time.toJavaInstant
import kotlin.time.toKotlinInstant

class LeaderElectorTest {
    val mockApi = MockCoordinationApi()

    private fun testElector(
        leaseName: String = "test-lease",
        identity: String,
        api: CoordinationApi = mockApi,
        leaseDuration: Duration = 1.seconds,
        renewalDelay: Duration = 250.milliseconds,
        clock: Clock = Clock.System,
        onFollower: (suspend () -> Unit) = { },
        onLeader: suspend () -> Unit,
    ) = LeaderElector(
        leaseName = leaseName,
        namespace = "ns1",
        identity = identity,
        api = api,
        leaseDuration = leaseDuration,
        renewalDelay = renewalDelay,
        clock = clock,
        onFollower = onFollower,
        onLeader = onLeader,
    )

    @Test
    fun `given a single elector it becomes leader`() {
        val channel = Channel<Boolean>()
        val elector = testElector(identity = "test-elector-1", onFollower = { channel.send(false) }) {
            channel.send(true)
        }

        runBlocking {
            elector.start()
            channel.receive() shouldBe true
            elector.stop()
        }
    }

    @Test
    fun `given many electors, only one becomes leader`() {
        val channel = Channel<Boolean>()
        suspend fun onFollower() { channel.send(false) }
        suspend fun onLeader() { channel.send(true) }

        val electors = listOf(
            testElector(identity = "test-elector-1", onFollower = ::onFollower, onLeader = ::onLeader),
            testElector(identity = "test-elector-2", onFollower = ::onFollower, onLeader = ::onLeader),
            testElector(identity = "test-elector-3", onFollower = ::onFollower, onLeader = ::onLeader)
        ).shuffled()

        runBlocking {
            electors.forEach { it.start() }
            val results = mutableListOf<Boolean>()
            electors.forEach { _ -> results.add(channel.receive()) }
            electors.forEach { it.stop() }

            results shouldContainExactlyInAnyOrder listOf(true, false, false)
        }
    }

    @Test
    fun `if the current leader stops another elector takes over`() {
        val channel = Channel<String>()

        val leader = testElector(identity = "test-elector-1", onFollower = { channel.send("follower-1") }) {
            channel.send("leader-1")
        }
        val electors = listOf(
            testElector(identity = "test-elector-2", onFollower = { channel.send("follower-2") }) {
                channel.send("leader-2")
            },
            testElector(identity = "test-elector-3", onFollower = { channel.send("follower-3")}) {
                channel.send("leader-3")
            }
        )

        runBlocking {
            // start leader, wait for election
            leader.start()
            channel.receive() shouldBe "leader-1"

            // start followers, wait for follow role
            electors.forEach { it.start() }
            val initialFollowers = setOf(channel.receive(), channel.receive())
            initialFollowers shouldContainExactlyInAnyOrder listOf("follower-2", "follower-3")

            // stop the leader
            leader.stop()

            // record the next leader election and verify its id
            var nextLeader = ""
            while (nextLeader.isEmpty()) {
                when (val event = channel.receive()) {
                    "leader-2", "leader-3" -> nextLeader = event
                }
            }
            setOf("leader-2", "leader-3").contains(nextLeader) shouldBe true

            // stop electors
            electors.forEach { it.stop() }
        }
    }

    @Test
    fun `a stopped elector can become leader again once restarted`() {
        val channel = Channel<Boolean>()
        val elector = testElector(identity = "test-elector-1", onFollower = { channel.send(false) }) {
            channel.send(true)
        }

        runBlocking {
            elector.start()
            channel.receive() shouldBe true
            elector.stop()

            elector.start()

            // wait for the elector to become the leader
            // if it does not become leader within 5 seconds the test fails
            shouldNotThrow<TimeoutCancellationException> {
                withTimeout(5.seconds) {
                    while (!channel.receive()) delay(50.milliseconds)
                }
            }

            elector.stop()
        }
    }

    @Test
    fun `an elector can only be started once`() {
        val channel = Channel<Boolean>()
        val elector = testElector(identity = "test-elector-1") {
            channel.send(true)
            delay(5.seconds)
        }

        runBlocking {
            repeat(10) {
                launch { elector.start() }
            }

            channel.receive() shouldBe true

            shouldThrow<TimeoutCancellationException> {
                withTimeout(1.seconds) {
                    channel.receiveCatching().getOrNull()
                }
            }

            elector.stop()
        }
    }

    @Test
    fun `an elector can be stopped multiple times`() {
        val channel = Channel<Boolean>()
        val elector = testElector(identity = "test-elector-1") {
            channel.send(true)
            delay(5.seconds)
        }

        runBlocking {
            elector.start()
            channel.receive() shouldBe true

            shouldNotThrowAny {
                repeat(10) {
                    launch { elector.stop() }
                }
            }
        }

        mockApi.callLog
            .filterIsInstance<ReplaceNamespacedLease>()
            .filter { it.response.spec?.holderIdentity==null }
            .size shouldBe 1
    }

    @Test
    fun `onLeader and onFollower are only called once per election`() {
        val electorChannel = Channel<Boolean>()
        val elector = testElector(
            identity = "test-elector-1",
            onFollower = { electorChannel.send(false) },
        ) {
            electorChannel.send(true)
        }

        val followerChannel = Channel<Boolean>()
        val follower = testElector(
            identity = "test-elector-2",
            onFollower = { followerChannel.send(false) },
        ) {
            followerChannel.send(true)
        }

        runBlocking {
            elector.start()
            electorChannel.receive() shouldBe true
            follower.start()
            followerChannel.receive() shouldBe false

            coroutineScope {
                launch {
                    shouldThrow<TimeoutCancellationException> {
                        withTimeout(1.seconds) {
                            electorChannel.receive()
                        }
                    }
                }
                launch {
                    shouldThrow<TimeoutCancellationException> {
                        withTimeout(1.seconds) {
                            followerChannel.receive()
                        }
                    }
                }
            }
        }
    }

    @Nested
    @DisplayName("API Spec")
    inner class ApiSpecTests {
        private val clock = object : Clock {
            override fun now() = Instant.fromEpochSeconds(0)
        }

        @Test
        fun `when lease is created payload matches contract`() {
            val channel = Channel<Boolean>()
            val leaseName = "create-lease"
            val elector = testElector(leaseName = leaseName, identity = "test-elector-1", clock = clock) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true
                elector.stop()
            }

            val createCall = mockApi.callLog.filterIsInstance<CreateNamespacedLease>().first()
            createCall.namespace shouldBe "ns1"
            createCall.lease.let { lease ->
                lease.metadata!!.let { metadata ->
                    metadata.name shouldBe leaseName
                    metadata.namespace shouldBe "ns1"
                }
                lease.spec!!.let { spec ->
                    spec.holderIdentity shouldBe "test-elector-1"
                    spec.leaseDurationSeconds shouldBe 1
                    spec.renewTime!!.toInstant().toKotlinInstant() shouldBe clock.now()
                }
            }
        }

        @Test
        fun `when leader renews replace payload matches contract`() {
            val channel = Channel<Boolean>()
            val leaseName = "renew-lease"
            val elector = testElector(leaseName = leaseName, identity = "test-elector-1", clock = clock) {
                channel.send(true)
            }

            var renewCall: ReplaceNamespacedLease? = null

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true


                // wait for the replace call to occur
                shouldNotThrow<TimeoutCancellationException> {
                    withTimeout(5.seconds) {
                        do {
                            delay(50.milliseconds)
                            renewCall = mockApi.callLog.filterIsInstance<ReplaceNamespacedLease>().firstOrNull {
                                it.leaseName == leaseName && it.lease.spec!!.holderIdentity == "test-elector-1"
                            }
                        } while (renewCall == null)
                    }
                }

                elector.stop()
            }

            renewCall!!.namespace shouldBe "ns1"
            renewCall.lease.let { lease ->
                lease.metadata!!.let { metadata ->
                    metadata.name shouldBe leaseName
                    metadata.namespace shouldBe "ns1"
                }
                lease.spec!!.let { spec ->
                    spec.holderIdentity shouldBe "test-elector-1"
                    spec.leaseDurationSeconds shouldBe 1
                    spec.renewTime!!.toInstant().toKotlinInstant() shouldBe clock.now()
                }
            }
        }

        @Test
        fun `when lease is expired replace payload claims contender identity`() {
            val channel = Channel<Boolean>()
            val leaseName = "expired-lease"

            // create and lock
            mockApi.createNamespacedLease(
                namespace = "ns1",
                lease = V1Lease()
                    .metadata(V1ObjectMeta().name(leaseName).namespace("ns1"))
                    .spec(
                        V1LeaseSpec()
                            .holderIdentity("other-elector")
                            .leaseDurationSeconds(1)
                            .renewTime(clock.now().toJavaInstant().atOffset(ZoneOffset.UTC).minusSeconds(2)),
                    ),
            )

            val elector = testElector(leaseName = leaseName, identity = "test-elector-2", clock = clock) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true
                elector.stop()
            }

            val acquireCall = mockApi.callLog.filterIsInstance<ReplaceNamespacedLease>()
                .first { it.leaseName == leaseName && it.lease.spec!!.holderIdentity == "test-elector-2" }
            acquireCall.namespace shouldBe "ns1"
            acquireCall.lease.let { lease ->
                lease.metadata!!.let { metadata ->
                    metadata.name shouldBe leaseName
                    metadata.namespace shouldBe "ns1"
                }
                lease.spec!!.let { spec ->
                    spec.holderIdentity shouldBe "test-elector-2"
                    spec.leaseDurationSeconds shouldBe 1
                    spec.renewTime!!.toInstant().toKotlinInstant() shouldBe clock.now()
                }
            }
        }

        @Test
        fun `leaseDuration is clamped to 1 second`() {
            val channel = Channel<Boolean>()
            val leaseName = "clamped-lease"
            val elector = testElector(
                leaseName = leaseName,
                identity = "test-elector-1",
                api = mockApi,
                leaseDuration = 100.milliseconds,
                renewalDelay = 25.milliseconds,
            ) {
                channel.send(true)
            }

            var renewCall: ReplaceNamespacedLease? = null
            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true

                shouldNotThrow<TimeoutCancellationException> {
                    withTimeout(5.seconds) {
                        do {
                            delay(25.milliseconds)
                            renewCall = mockApi.callLog.filterIsInstance<ReplaceNamespacedLease>().firstOrNull {
                                it.leaseName == leaseName && it.lease.spec!!.holderIdentity == "test-elector-1"
                            }
                        } while (renewCall == null)
                    }
                }

                elector.stop()
            }

            val createCall = mockApi.callLog.filterIsInstance<CreateNamespacedLease>().first {
                it.lease.metadata!!.name == leaseName
            }
            createCall.lease.spec!!.leaseDurationSeconds shouldBe 1
            renewCall!!.lease.spec!!.leaseDurationSeconds shouldBe 1
        }

        @Test
        fun `if the current leader is stopped it releases the held lease`() {
            val clock = object : Clock {
                override fun now() = Instant.fromEpochSeconds(0)
            }
            val channel = Channel<Boolean>()
            val leaseName = "release-lease"
            val elector = testElector(leaseName = leaseName, identity = "test-elector-1", clock = clock) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true
                elector.stop()
            }

            val releaseCall = mockApi.callLog.last() as ReplaceNamespacedLease
            releaseCall.leaseName shouldBe leaseName
            releaseCall.namespace shouldBe "ns1"
            releaseCall.lease.let { lease ->
                lease.metadata!!.let { metadata ->
                    metadata.name shouldBe leaseName
                    metadata.namespace shouldBe "ns1"
                    (metadata.resourceVersion!!.toInt() > 1) shouldBe true
                }

                lease.spec!!.let { spec ->
                    spec.holderIdentity shouldBe null
                    spec.leaseDurationSeconds shouldBe 1
                    spec.renewTime!!.toInstant().toKotlinInstant() shouldBe clock.now()
                }
            }
        }
    }

    @Nested
    @DisplayName("Error Handling")
    inner class ErrorHandlingTests {
        @Test
        fun `when read gets unknown api error elector retries and eventually becomes leader`() {
            var readCount = 0
            val api = object : CoordinationApi by mockApi {
                override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease {
                    readCount += 1
                    if (readCount == 1) {
                        throw ApiException(0, "unknown")
                    }
                    return mockApi.readNamespacedLease(leaseName, namespace)
                }
            }

            val channel = Channel<Boolean>()
            val elector = testElector(
                leaseName = "unknown-read-lease",
                identity = "test-elector-1",
                api = api,
            ) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true
                (readCount > 1) shouldBe true
                elector.stop()
            }
        }

        @Test
        fun `when create gets unknown api error elector retries and eventually becomes leader`() {
            var createCount = 0
            val api = object : CoordinationApi by mockApi {
                override fun createNamespacedLease(namespace: String, lease: V1Lease): V1Lease {
                    createCount += 1
                    if (createCount == 1) {
                        throw ApiException(0, "unknown")
                    }
                    return mockApi.createNamespacedLease(namespace, lease)
                }
            }

            val channel = Channel<Boolean>()
            val elector = testElector(
                leaseName = "unknown-create-lease",
                identity = "test-elector-1",
                api = api,
            ) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true
                (createCount > 1) shouldBe true
                elector.stop()
            }
        }

        @Test
        fun `when replace gets unknown api error elector remains leader and retries`() {
            var replaceCount = 0
            val api = object : CoordinationApi by mockApi {
                override fun replaceNamespacedLease(leaseName: String, namespace: String, lease: V1Lease): V1Lease {
                    replaceCount += 1
                    if (replaceCount == 1) {
                        throw ApiException(0, "unknown")
                    }
                    return mockApi.replaceNamespacedLease(leaseName, namespace, lease)
                }
            }

            val channel = Channel<Boolean>()
            val elector = testElector(
                leaseName = "unknown-replace-lease",
                identity = "test-elector-1",
                api = api,
                onFollower = { channel.send(false) },
            ) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true
                shouldThrow<TimeoutCancellationException> {
                    withTimeout(1.seconds) {
                        channel.receive()
                    }
                }
                (replaceCount > 1) shouldBe true
                elector.stop()
            }
        }

        @Test
        fun `when read gets transient error elector retries and eventually becomes leader`() {
            var readCount = 0
            val api = object : CoordinationApi by mockApi {
                override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease {
                    readCount += 1
                    if (readCount == 1) {
                        throw ApiException(500, "transient")
                    }
                    return mockApi.readNamespacedLease(leaseName, namespace)
                }
            }

            val channel = Channel<Boolean>()
            val elector = testElector(
                leaseName = "transient-read-lease",
                identity = "test-elector-1",
                api = api,
            ) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true
                (readCount > 1) shouldBe true
                elector.stop()
            }
        }

        @Test
        fun `when create conflicts elector does not become leader`() {
            val api = object : CoordinationApi {
                override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease =
                    throw ApiException(404, "not found")

                override fun createNamespacedLease(namespace: String, lease: V1Lease): V1Lease =
                    throw ApiException(409, "already exists")

                override fun replaceNamespacedLease(leaseName: String, namespace: String, lease: V1Lease): V1Lease =
                    throw ApiException(500, "should not be called")
            }

            val channel = Channel<Boolean>()
            val elector = testElector(
                leaseName = "create-conflict-lease",
                identity = "test-elector-1",
                api = api,
                onFollower = { channel.send(false) },
            ) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe false

                elector.stop()
            }
        }

        @Test
        fun `when replace conflicts elector loses leadership`() {
            var replaceCount = 0
            val api = object : CoordinationApi by mockApi {
                override fun replaceNamespacedLease(leaseName: String, namespace: String, lease: V1Lease): V1Lease {
                    replaceCount += 1
                    if (replaceCount == 1) {
                        throw ApiException(409, "already modified")
                    }
                    return mockApi.replaceNamespacedLease(leaseName, namespace, lease)
                }
            }

            val channel = Channel<Boolean>()
            val elector = testElector(
                leaseName = "replace-conflict-lease",
                identity = "test-elector-1",
                api = api,
                onFollower = { channel.send(false) },
            ) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true

                var demoted = false
                withTimeout(5.seconds) {
                    while (!demoted) {
                        demoted = !channel.receive()
                    }
                }
                demoted shouldBe true
                elector.stop()
            }
        }

        @Test
        fun `when read gets auth error elector loop exits`() {
            var readCount = 0
            val api = object : CoordinationApi {
                override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease {
                    readCount += 1
                    throw ApiException(401, "unauthorized")
                }

                override fun createNamespacedLease(namespace: String, lease: V1Lease): V1Lease =
                    throw ApiException(500, "should not be called")

                override fun replaceNamespacedLease(leaseName: String, namespace: String, lease: V1Lease): V1Lease =
                    throw ApiException(500, "should not be called")
            }

            val elector = testElector(
                leaseName = "auth-error-lease",
                identity = "test-elector-1",
                api = api,
            ) { }

            runBlocking {
                elector.start()
                delay(500.milliseconds)
                readCount shouldBe 1
                elector.stop()
            }
        }

        @Test
        fun `when read throws unexpected exception elector loop exits`() {
            var readCount = 0
            val api = object : CoordinationApi {
                override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease {
                    readCount += 1
                    throw IllegalStateException("boom")
                }

                override fun createNamespacedLease(namespace: String, lease: V1Lease): V1Lease =
                    throw ApiException(500, "should not be called")

                override fun replaceNamespacedLease(leaseName: String, namespace: String, lease: V1Lease): V1Lease =
                    throw ApiException(500, "should not be called")
            }

            val elector = testElector(
                leaseName = "throwable-error-lease",
                identity = "test-elector-1",
                api = api,
            ) { }

            runBlocking {
                elector.start()
                delay(500.milliseconds)
                readCount shouldBe 1
                elector.stop()
            }
        }

        @Test
        fun `when release read is not found stop ignores it`() {
            var failReleaseRead = false
            val api = object : CoordinationApi by mockApi {
                override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease {
                    if (failReleaseRead) {
                        throw ApiException(404, "not found")
                    }
                    return mockApi.readNamespacedLease(leaseName, namespace)
                }
            }

            val channel = Channel<Boolean>()
            val elector = testElector(
                leaseName = "release-404-lease",
                identity = "test-elector-1",
                api = api,
            ) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true

                failReleaseRead = true
                shouldNotThrow<ApiException> {
                    elector.stop()
                }
            }
        }

        @Test
        fun `when release read gets unknown api error stop throws`() {
            var failReleaseRead = false
            val api = object : CoordinationApi by mockApi {
                override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease {
                    if (failReleaseRead) {
                        throw ApiException(0, "unknown")
                    }
                    return mockApi.readNamespacedLease(leaseName, namespace)
                }
            }

            val channel = Channel<Boolean>()
            val elector = testElector(
                leaseName = "release-unknown-lease",
                identity = "test-elector-1",
                api = api,
            ) {
                channel.send(true)
            }

            runBlocking {
                elector.start()
                withTimeout(5.seconds) { channel.receive() } shouldBe true

                failReleaseRead = true
                val exception = shouldThrow<ApiException> {
                    elector.stop()
                }
                exception.code shouldBe 0
            }
        }
    }
}

class MockCoordinationApi : CoordinationApi {
    private val lock = ReentrantLock()
    private val leases = mutableMapOf<String, V1Lease>()
    private var version: Int = 0

    private val backingCallLog = mutableListOf<CoordinationApiCall>()
    val callLog: List<CoordinationApiCall> get() { return backingCallLog.toList() }

    override fun readNamespacedLease(leaseName: String, namespace: String): V1Lease {
        return lock.withLock {
            (leases["$namespace/$leaseName"] ?: throw ApiException(404, "not found"))
                .also { backingCallLog.add(ReadNamespacedLease(leaseName, namespace, it)) }
        }
    }

    override fun createNamespacedLease(namespace: String, lease: V1Lease): V1Lease {
        return lock.withLock {
            lease.metadata!!.resourceVersion = (++version).toString()

            if (leases.containsKey("$namespace/${lease.metadata!!.name}")) {
                throw ApiException(409, "already exists")
            } else {
                leases["$namespace/${lease.metadata!!.name}"] = lease
                lease.also { backingCallLog.add(CreateNamespacedLease(namespace, lease, it)) }
            }
        }
    }

    override fun replaceNamespacedLease(leaseName: String, namespace: String, lease: V1Lease): V1Lease {
        return lock.withLock {
            val existingLease = leases["$namespace/$leaseName"] ?: throw ApiException(404, "not found")

            if (existingLease.metadata!!.resourceVersion != lease.metadata!!.resourceVersion) {
                throw ApiException(409, "already modified")
            } else {
                lease.metadata!!.resourceVersion = (++version).toString()
                leases["$namespace/$leaseName"] = lease
                lease.also { backingCallLog.add(ReplaceNamespacedLease(leaseName, namespace, lease, it)) }
            }
        }
    }
}

sealed interface CoordinationApiCall {
    val response: V1Lease
}

data class ReadNamespacedLease(
    val leaseName: String,
    val namespace: String,
    override val response: V1Lease
) : CoordinationApiCall

data class CreateNamespacedLease(
    val namespace: String,
    val lease: V1Lease,
    override val response: V1Lease
) : CoordinationApiCall

data class ReplaceNamespacedLease(
    val leaseName: String,
    val namespace: String,
    val lease: V1Lease,
    override val response: V1Lease
) : CoordinationApiCall
