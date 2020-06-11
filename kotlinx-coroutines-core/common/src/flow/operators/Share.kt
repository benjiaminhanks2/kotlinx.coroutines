/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

@file:JvmMultifileClass
@file:JvmName("FlowKt")

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.internal.*
import kotlin.coroutines.*
import kotlin.jvm.*

// -------------------------------- shareIn --------------------------------

/**
 * Converts a _cold_ [Flow] into a _hot_ [SharedFlow] that is started in the given coroutine [scope],
 * sharing emissions from a single running instance of upstream flow with multiple downstream subscribers,
 * and replaying a specified number of [replay] values to new subscribers. See [SharedFlow] documentation
 * on a general concepts of shared flows.
 *
 * Start of the sharing coroutine is controlled by the [started] parameter. By default, the sharing coroutine is started
 * [Eagerly][SharingStarted.Eagerly], so upstream flow is started even before the first subscribers appears and all
 * the values emitted by upstream beyond the specified [replay] parameter **will be immediately discarded**.
 * Additional options for [started] parameter are:
 *
 * * [Lazily][SharingStarted.Lazily] &mdash; starts the upstream flow after the first subscriber appears, which guarantees
 *   that this first subscriber gets all the emitted values, while subsequence subscribers are only guaranteed to
 *   get the most recent [replay] values. The upstream flow continues to be active even when all subscribers
 *   disappear and only the most recent [replay] values are stored without subscribers.
 * * [WhileSubscribed()][SharingStarted.WhileSubscribed] &mdash; starts the upstream flow when the first subscriber
 *   appears, immediately stops when the last subscriber disappears, keeping the replay cache forever.
 *   It has additional optional configuration parameters as explained in its documentation.
 * * Custom strategy can be supplied by implementing [SharingStarted] interface.
 *
 * `shareIn` operator is useful in situations when there is a _cold_ flow that is expensive to create and/or
 * to maintain but there are multiple subscribers that need to collect its values. For example, consider a
 * flow of messages coming from a backend over the expensive network connection, taking a lot of
 * time to establish. Conceptually it might be implemented like this:
 *
 * ```
 * val backendMessages: Flow<Message> = flow {
 *     connectToBackend() // takes a lot of time
 *     try {
 *       while (true) {
 *           emit(receiveMessageFromBackend())
 *       }
 *     } finally {
 *         disconnectFromBackend()
 *     }
 * }
 * ```
 *
 * If this flow is directly used in the application, then every time it is collected a fresh connection is
 * established and it will take a while before messages start flowing. However, we can share a single connection
 * and establish it eagerly like this:
 *
 * ```
 * val messages: SharedFlow<Message> = backendMessages.shareIn(scope, 0)
 * ```
 *
 * Now, a single connection is shared between all collectors from `messages` and there is a chance that the connection
 * is already established by the time it is needed.
 *
 * ### Error handling
 *
 * Any exception in the upstream flow terminates the sharing coroutine without affecting any of the subscribers
 * and will be handled by the [scope] in which the sharing coroutine is launched. Custom exception handling
 * can be configured by using [catch] or [retry] operators before `shareIn` operator.
 * For example, to retry connection on any `IOException` with 1 second delay between attempts use:
 *
 * ```
 * val messages = backendMessages
 *     .retry { e ->
 *         val shallRetry = e is IOException // other exception are bugs - handle them
 *         if (shallRetry) delay(1000)
 *         shallRetry
 *     }
 *     .shareIn(scope, 0)
 * ```
 *
 * ### Buffering and conflation
 *
 * The `shareIn` operator runs upstream flow in a separate coroutine and buffers emissions from upstream as explained
 * in [buffer] operator description using a buffer of [replay] size or the default (whichever is larger).
 * This default buffering can be overridden with an explicit buffer configuration by preceding `shareIn` call
 * with [buffer] or [conflate], for example:
 *
 * * `buffer(0).shareIn(scope, 0)` &mdash; overrides a default buffer size and creates a [SharedFlow] without a buffer.
 *   Effectively, it configures sequential processing between upstream emitter and subscribers,
 *   as emitter is suspended until all subscribers process the value. Note, that the value is still immediately
 *   discarded when there are no subscribers.
 * * `buffer(b).shareIn(scope, r)` &mdash; creates a [SharedFlow] with `replay = r` and `extraBufferCapacity = b`.
 * * `conflate().shareIn(scope, r)` &mdash; creates a [SharedFlow] with `replay = r`, `onBufferOverflow = DROP_OLDEST`,
 *   and `extraBufferCapacity = 1` when `replay == 0` to support this strategy.
 *
 * ### Operator fusion
 *
 * Application of [flowOn][Flow.flowOn], [buffer] with [RENDEZVOUS][Channel.RENDEZVOUS] capacity,
 * or [cancellable] operators to the resulting shared flow has no effect.
 *
 * ### Exceptions
 *
 * This function throws [IllegalArgumentException] on unsupported values of parameters of combinations thereof.
 *
 * @param scope the coroutine scope in which sharing is started.
 * @param replay the number of values replayed to new subscribers (cannot be negative).
 * @param started the strategy that controls when sharing is started and stopped
 *   (optional, default to [Eagerly][SharingStarted.Eagerly] starting the sharing without waiting for subscribers).
 * @param initialValue the initial value in the replay cache (optional, defaults to nothing, supported only when `replay > 0`).
 *   This value is also used when shared flow buffer is reset using [SharingStarted.WhileSubscribed] strategy
 *   with `replayExpirationMillis` parameter.
 */
@ExperimentalCoroutinesApi
public fun <T> Flow<T>.shareIn(
    scope: CoroutineScope,
    replay: Int,
    started: SharingStarted = SharingStarted.Eagerly,
    initialValue: T = NO_VALUE as T
): SharedFlow<T> {
    val config = configureSharing(replay)
    val shared = MutableSharedFlow<T>(
        replay = replay,
        extraBufferCapacity = config.extraBufferCapacity,
        onBufferOverflow = config.onBufferOverflow,
        initialValue = initialValue
    )
    scope.launchSharing(config.context, config.upstream, shared, started)
    return shared.asSharedFlow()
}

private class SharingConfig<T>(
    @JvmField val upstream: Flow<T>,
    @JvmField val extraBufferCapacity: Int,
    @JvmField val onBufferOverflow: BufferOverflow,
    @JvmField val context: CoroutineContext
)

// Decomposes upstream flow to fuse with it when possible
private fun <T> Flow<T>.configureSharing(replay: Int): SharingConfig<T> {
    assert { replay >= 0 }
    val defaultExtraCapacity = replay.coerceAtLeast(Channel.CHANNEL_DEFAULT_CAPACITY) - replay
    // Combine with preceding buffer/flowOn and channel-using operators
    if (this is ChannelFlow) {
        // Check if this ChannelFlow can operate without a channel
        val upstream = dropChannelOperators()
        if (upstream != null) { // Yes, it can => eliminate the intermediate channel
            return SharingConfig(
                upstream = upstream,
                extraBufferCapacity = when (capacity) {
                    Channel.OPTIONAL_CHANNEL, Channel.BUFFERED, 0 -> // handle special capacities
                        when {
                            onBufferOverflow == BufferOverflow.SUSPEND -> // buffer was configured with suspension
                                if (capacity == 0) 0 else defaultExtraCapacity // keep explicitly configure 0 or use default
                            replay == 0 -> 1 // no suspension => need at least buffer of one
                            else -> 0 // replay > 0 => no need for extra buffer beyond replay because we don't suspend
                        }
                    else -> capacity // otherwise just use the specified capacity as extra capacity
                },
                onBufferOverflow = onBufferOverflow,
                context = context
            )
        }
    }
    // Add sharing operator on top with a default buffer
    return SharingConfig(
        upstream = this,
        extraBufferCapacity = defaultExtraCapacity,
        onBufferOverflow = BufferOverflow.SUSPEND,
        context = EmptyCoroutineContext
    )
}

// Launches sharing coroutine
private fun <T> CoroutineScope.launchSharing(
    context: CoroutineContext,
    upstream: Flow<T>,
    shared: MutableSharedFlow<T>,
    started: SharingStarted
) {
    launch(context) { // the single coroutine to rule the sharing
        try {
            started.command(shared.subscriptionCount)
                .distinctUntilChanged()
                .collectLatest { // cancels block on new emission
                    when (it) {
                        SharingCommand.START -> upstream.collect(shared) // can be cancelled
                        SharingCommand.STOP -> { /* just cancel and do nothing else */ }
                        SharingCommand.STOP_AND_RESET_REPLAY_CACHE -> shared.resetReplayCache()
                    }
                }
        } finally {
            shared.resetReplayCache() // on any completion/cancellation/failure of sharing
        }
    }
}

// -------------------------------- stateIn --------------------------------

/**
 * Converts a _cold_ [Flow] into a _hot_ [StateFlow] that is started in the given coroutine [scope],
 * sharing the most recently emitted value by single running instance of upstream flow with multiple
 * downstream subscribers. See [StateFlow] documentation on a general concepts of state flows.
 *
 * Start of the sharing coroutine is controlled by the [started] parameter as explained in the
 * documentation for [shareIn] operator.
 *
 * `stateIn` operator is useful in situations when there is a _cold_ flow that provides updates to the
 * value of some state and is expensive to create and/or to maintain but there are multiple subscribers
 * that need to collect the most recent state value. For example, consider a
 * flow of state updates coming from a backend over the expensive network connection, taking a lot of
 * time to establish. Conceptually it might be implemented like this:
 *
 * ```
 * val backendState: Flow<State> = flow {
 *     connectToBackend() // takes a lot of time
 *     try {
 *       while (true) {
 *           emit(receiveStateUpdateFromBackend())
 *       }
 *     } finally {
 *         disconnectFromBackend()
 *     }
 * }
 * ```
 *
 * If this flow is directly used in the application, then every time it is collected a fresh connection is
 * established and it will take a while before state updates start flowing. However, we can share a single connection
 * and establish it eagerly like this:
 *
 * ```
 * val state: StateFlow<State> = backendMessages.stateIn(scope, initialValue = State.LOADING)
 * ```
 *
 * Now, a single connection is shared between all collectors from `state` and there is a chance that the connection
 * is already established by the time it is needed.
 *
 * ### Error handling
 *
 * Any exception in the upstream flow terminates the sharing coroutine without affecting any of the subscribers
 * and will be handled by the [scope] in which the sharing coroutine is launched. Custom exception handling
 * can be configured by using [catch] or [retry] operators before `stateIn` operator similarly to
 * the [shareIn] operator.
 *
 * ### Operator fusion
 *
 * Application of [flowOn][Flow.flowOn], [conflate][Flow.conflate],
 * [buffer] with [CONFLATED][Channel.CONFLATED] or [RENDEZVOUS][Channel.RENDEZVOUS] capacity,
 * [distinctUntilChanged][Flow.distinctUntilChanged], or [cancellable] operators to a state flow has no effect.
 *
 * @param scope the coroutine scope in which sharing is started.
 * @param started the strategy that controls when sharing is started and stopped
 *   (optional, default to [Eagerly][SharingStarted.Eagerly] starting the sharing without waiting for subscribers).
 * @param initialValue the initial value of the state flow.
 *   This value is also used when state flow is reset using [SharingStarted.WhileSubscribed] strategy
 *   with `replayExpirationMillis` parameter.
 */
@ExperimentalCoroutinesApi
public fun <T> Flow<T>.stateIn(
    scope: CoroutineScope,
    started: SharingStarted = SharingStarted.Eagerly,
    initialValue: T
): StateFlow<T> {
    val config = configureSharing(1)
    val state = MutableStateFlow(initialValue)
    scope.launchSharing(config.context, config.upstream, state, started)
    return state.asStateFlow()
}

/**
 * Starts the upstream flow in a given [scope], suspends until the first value is emitted, and returns a _hot_
 * [StateFlow] of future emissions, sharing the most recently emitted value by this running instance of upstream flow
 * with multiple downstream subscribers. See [StateFlow] documentation on a general concepts of state flows.
 *
 * @param scope the coroutine scope in which sharing is started.
 */
@ExperimentalCoroutinesApi
public suspend fun <T> Flow<T>.stateIn(scope: CoroutineScope): StateFlow<T> {
    val config = configureSharing(1)
    val result = CompletableDeferred<StateFlow<T>>()
    scope.launchSharingDeferred(config.context, config.upstream, result)
    return result.await()
}

private fun <T> CoroutineScope.launchSharingDeferred(
    context: CoroutineContext,
    upstream: Flow<T>,
    result: CompletableDeferred<StateFlow<T>>
) {
    launch(context) {
        var state: MutableStateFlow<T>? = null
        upstream.collect { value ->
            state?.let { it.value = value } ?: run {
                state = MutableStateFlow(value).also {
                    result.complete(it.asStateFlow())
                }
            }
        }
    }
}

// -------------------------------- asSharedFlow/asStateFlow --------------------------------

/**
 * Represents this mutable shared flow as read-only shared flow.
 */
@ExperimentalCoroutinesApi
public fun <T> MutableSharedFlow<T>.asSharedFlow(): SharedFlow<T> =
    ReadonlySharedFlow(this)

/**
 * Represents this mutable state flow as read-only state flow.
 */
@ExperimentalCoroutinesApi
public fun <T> MutableStateFlow<T>.asStateFlow(): StateFlow<T> =
    ReadonlyStateFlow(this)

private class ReadonlySharedFlow<T>(
    flow: SharedFlow<T>
) : SharedFlow<T> by flow, CancellableFlow<T>, FusibleFlow<T> {
    override fun fuse(context: CoroutineContext, capacity: Int, onBufferOverflow: BufferOverflow) =
        fuseSharedFlow(context, capacity, onBufferOverflow)
}

private class ReadonlyStateFlow<T>(
    flow: StateFlow<T>
) : StateFlow<T> by flow, CancellableFlow<T>, FusibleFlow<T>, DistinctFlow<T> {
    override val isDefaultEquivalence: Boolean
        get() = true

    override fun fuse(context: CoroutineContext, capacity: Int, onBufferOverflow: BufferOverflow) =
        fuseStateFlow(context, capacity, onBufferOverflow)
}

// -------------------------------- onSubscription --------------------------------

/**
 * Returns a flow that invokes the given [action] **after** this shared flow starts to be collected
 * (after subscription is registered). The [action] is called before any value is emitted from the upstream
 * flow to this subscription yet, but it is guaranteed that all future emissions to the upstream flow will be
 * collected by this subscription.
 *
 * The receiver of the [action] is [FlowCollector], so `onSubscription` can emit additional elements.
 */
@ExperimentalCoroutinesApi
public fun <T> SharedFlow<T>.onSubscription(action: suspend FlowCollector<T>.() -> Unit): SharedFlow<T> =
    SubscribedSharedFlow(this, action)

private class SubscribedSharedFlow<T>(
    private val sharedFlow: SharedFlow<T>,
    private val action: suspend FlowCollector<T>.() -> Unit
) : SharedFlow<T> by sharedFlow {
    override suspend fun collect(collector: FlowCollector<T>) =
        sharedFlow.collect(SubscribedFlowCollector(collector, action))
}

internal class SubscribedFlowCollector<T>(
    private val collector: FlowCollector<T>,
    private val action: suspend FlowCollector<T>.() -> Unit
) : FlowCollector<T> by collector {
    suspend fun onSubscription() {
        val safeCollector = SafeCollector(collector, coroutineContext)
        try {
            safeCollector.action()
        } finally {
            safeCollector.releaseIntercepted()
        }
        if (collector is SubscribedFlowCollector) collector.onSubscription()
    }
}

