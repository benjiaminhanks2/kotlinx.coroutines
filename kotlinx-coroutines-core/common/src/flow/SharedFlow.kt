/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.flow.internal.*
import kotlinx.coroutines.internal.*
import kotlin.coroutines.*
import kotlin.jvm.*
import kotlin.native.concurrent.*

/**
 * A _hot_ [Flow] that shares emitted values among all its collectors in a broadcast fashion, so that all collectors
 * get all emitted values. A shared flow is called _hot_ because its active instance exists independently of the
 * presence of collectors. This is opposed to a regular [Flow], such as defined by the [`flow { ... }`][flow] function,
 * which is _cold_ and is started separately for each collector.
 *
 * **Shared flow never completes**. A call to [Flow.collect] on a shared flow never completes normally and
 * so does a coroutine started by [Flow.launchIn] function. An active collector of a shared flow is called a _subscriber_.
 *
 * A subscriber of a shared flow can be cancelled, which usually happens when the scope the coroutine is running
 * in is cancelled. A subscriber to a shared flow in always [cancellable][Flow.cancellable] and checks for
 * cancellation before each emission. Note that most terminal operators like [Flow.toList] would not complete, too,
 * when applied to a shared flow, but flow-truncating operators like [Flow.take] and [Flow.takeWhile] can be used on a
 * shared flow to turn it into a completing one.
 *
 * A [mutable shared flow][MutableSharedFlow] is created using [MutableSharedFlow(...)] constructor function.
 * Its state can be updated by [emitting][MutableSharedFlow.emit] values to it and performing other operations.
 * See [MutableSharedFlow] documentation for details.
 *
 * [SharedFlow] is useful to broadcast events that happens inside application to subscribers that can come and go.
 * For example, the following class encapsulates an event bus that distributes events to all subscribers
 * in _rendezvous_ manner, suspending until all subscribers process each event:
 *
 * ```
 * class EventBus {
 *     private val _events = MutableSharedFlow<Event>(0) // private mutable shared flow
 *     val events get() = _events.asSharedFlow() // publicly exposed as read-only shared flow
 *
 *     suspend fun produceEvent(event: Event) {
 *         _events.emit(event) // suspends until all subscribers receive it
 *     }
 * }
 * ```
 *
 * As an alternative to the above usage with `MutableSharedFlow(...)` constructor function,
 * any _cold_ [Flow] can be converted to a shared flow using [shareIn] operator.
 *
 * There is a specialized implementation of shared flow for a case where the most recent state value needs
 * to be shared. See [StateFlow] for details.
 *
 * ### Replay cache and buffer
 *
 * A shared flow keeps a specific number of the most recent values in its _replay cache_. Every new subscribers first
 * gets the values from the replay cache and then gets new emitted values. The maximal size of the replay cache is
 * specified when the shared flow is created by the `replay` parameter. A snapshot of the current replay cache
 * is available via [replayCache] property.
 *
 * A replay cache provides buffer for emissions to the shared flow. Buffer space allows slow subscribers to
 * get values from the buffer without suspending emitters. The buffer space determines how much slow subscribers
 * can lag from the fast ones. When creating a shared flow additional buffer capacity beyond replay can be reserved
 * using `extraBufferCapacity` parameter.
 * 
 * A shared flow with a buffer can be configured to avoid suspension of emitters on buffer overflow using
 * `onBufferOverflow` parameter, which is equal to one of the entries of [BufferOverflow] enum. When a strategy other
 * than [SUSPENDED][BufferOverflow.SUSPEND] is configured emissions to the shared flow never suspend.
 *
 * ### SharedFlow vs BroadcastChannel
 *
 * Conceptually shared flow is similar to [BroadcastChannel][BroadcastChannel]
 * and is designed to completely replace `BroadcastChannel` in the future.
 * It has the following important differences:
 *
 * * `SharedFlow` is simpler, because it does not have to implement all the [Channel] APIs, which allows
 *    for faster and simpler implementation.
 * * `SharedFlow` supports configurable replay and buffer overflow strategy.
 * * `SharedFlow` has a clear separation into a read-only `SharedFlow` interface and a [MutableSharedFlow].
 * * `SharedFlow` cannot be closed like `BroadcastChannel` and can never represent a failure.
 *    All errors and completion signals shall be explicitly _materialized_ if needed.
 *
 * To migrate [BroadcastChannel] usage to [SharedFlow] start by replacing `BroadcastChannel(capacity)`
 * constructor with `MutableSharedFlow(0, extraBufferCapacity=capacity)` (broadcast channel does not replay
 * values to new subscribers). Replace [send][BroadcastChannel.send] and [offer][BroadcastChannel.offer] calls
 * with [emit][MutableStateFlow.emit] and [tryEmit][MutableStateFlow.tryEmit], and convert subscribers' code to flow operators.
 *
 * ### Concurrency
 *
 * All methods of shared flow are **thread-safe** and can be safely invoked from concurrent coroutines without
 * external synchronization.
 *
 * ### Operator fusion
 *
 * Application of [flowOn][Flow.flowOn], [buffer] with [RENDEZVOUS][Channel.RENDEZVOUS] capacity,
 * or [cancellable] operators to a shared flow has no effect.
 *
 * ### Implementation notes
 *
 * Shared flow implementation uses a lock to ensure thread-safety, but suspending collector and emitter coroutines are
 * resumed outside of this lock to avoid dead-locks when using unconfined coroutines. Adding new subscribers
 * has `O(1)` amortized cost, but emitting has `O(N)` cost, where `N` is the number of subscribers.
 *
 * ### Not stable for inheritance
 *
 * **`SharedFlow` interface is not stable for inheritance in 3rd party libraries**, as new methods
 * might be added to this interface in the future, but is stable for use.
 * Use `MutableSharedFlow(replay, ...)` constructor function to create an implementation.
 */
@ExperimentalCoroutinesApi
public interface SharedFlow<out T> : Flow<T> {
    /**
     * A snapshot of the replay cache.
     */
    public val replayCache: List<T>
}

/**
 * A mutable [SharedFlow] that provides functions to [emit] values to the flow.
 * Its instance with the given configuration parameters can be created using `MutableSharedFlow(...)`
 * constructor function.
 *
 * See [SharedFlow] documentation for details on shared flows.
 *
 * `MutableSharedFlow` is a [SharedFlow] that also provides abilities to [emit] a value,
 * to [tryEmit] without suspension if possible, to track the [subscriptionCount],
 * and to [resetReplayCache] to its initial state.
 *
 * ### Concurrency
 *
 * All methods of shared flow are **thread-safe** and can be safely invoked from concurrent coroutines without
 * external synchronization.
 *
 * ### Not stable for inheritance
 *
 * **`MutableSharedFlow` interface is not stable for inheritance in 3rd party libraries**, as new methods
 * might be added to this interface in the future, but is stable for use.
 * Use `MutableSharedFlow(...)` constructor function to create an implementation.
 */
@ExperimentalCoroutinesApi
public interface MutableSharedFlow<T> : SharedFlow<T>, FlowCollector<T> {
    /**
     * Tries to emit a [value] to this shared flow without suspending. It returns `true` if the value was
     * emitted successfully. When this function returns `false` it means that the call to a plain [emit]
     * function will suspend until there is a buffer space available.
     *
     * A shared flow configured with [BufferOverflow] strategy other than [SUSPEND][BufferOverflow.SUSPEND]
     * (either [DROP_OLDEST][BufferOverflow.DROP_OLDEST] or [DROP_LATEST][BufferOverflow.DROP_LATEST]) never
     * suspends on [emit] and thus `tryEmit` to such a shared flow always returns `true`.
     */
    public fun tryEmit(value: T): Boolean

    /**
     * A number of subscribers (active collectors) to this shared flow.
     *
     * This state can be used to react to changes in the number of subscriptions to this shared flow.
     * For example, if you need to call `onActive` function when the first subscriber appears and `onInactive`
     * function when the last one disappears, you can set it up like this:
     *
     * ```
     * sharedFlow.subscriptionCount
     *     .map { count -> count > 0 } // map count into active/inactive flag
     *     .distinctUntilChanged() // only react to true<->false changes
     *     .onEach { isActive -> // configure an action
     *         if (isActive) onActive() else onInactive()
     *     }
     *     .launchIn(scope) // launch it
     * ```
     */
    public val subscriptionCount: StateFlow<Int>

    /**
     * Resets [replayCache] of this shared flow to its initial value (if it was specified) or to the empty state.
     * New subscribers will be receiving only values emitted after this point in time, while old subscribers
     * might be still receiving previously buffered values.
     *
     * This function is idempotent. Calling it second time in a row has no effect.
     */
    public fun resetReplayCache()
}

/**
 * Creates a [MutableSharedFlow] with the given configuration parameters.
 *
 * This function throws [IllegalArgumentException] on unsupported values of parameters of combinations thereof.
 *
 * @param replay the number of values replayed to new subscribers (cannot be negative).
 * @param extraBufferCapacity the number of values buffered in addition to `replay`.
 *   [emit][SharedFlow.emit] does not suspend while there is a buffer space remaining (optional, cannot be negative, defaults to zero).
 * @param onBufferOverflow configures an action on buffer overflow (optional, defaults to
 *   [suspending][BufferOverflow.SUSPEND] attempt to [emit][MutableSharedFlow.emit] a value,
 *   supported only when `replay > 0` or `extraBufferCapacity > 0`).
 * @param initialValue the initial value in the replay cache (optional, defaults to nothing, supported only when `replay > 0`).
 *   This value is also used when shared flow buffer is [reset][MutableSharedFlow.reset].
 */
@Suppress("FunctionName", "UNCHECKED_CAST")
@ExperimentalCoroutinesApi
public fun <T> MutableSharedFlow(
    replay: Int,
    extraBufferCapacity: Int = 0,
    onBufferOverflow: BufferOverflow = BufferOverflow.SUSPEND,
    initialValue: T = NO_VALUE as T
): MutableSharedFlow<T> {
    require(replay >= 0) { "replay cannot be negative" }
    require(extraBufferCapacity >= 0) { "extraBufferCapacity cannot be negative" }
    require(replay > 0 || initialValue === NO_VALUE) { "replay must be positive with initialValue" }
    require(replay > 0 || extraBufferCapacity > 0 || onBufferOverflow == BufferOverflow.SUSPEND) {
        "replay or extraBufferCapacity must be positive with non-default onBufferOverflow strategy"
    }
    val bufferCapacity0 = replay + extraBufferCapacity
    val bufferCapacity = if (bufferCapacity0 < 0) Int.MAX_VALUE else bufferCapacity0 // coerce to MAX_VALUE on overflow
    return SharedFlowImpl(replay, bufferCapacity, onBufferOverflow, initialValue)
}

// ------------------------------------ Implementation ------------------------------------

private class SharedFlowSlot : AbstractSharedFlowSlot<SharedFlowImpl<*>>() {
    @JvmField
    var index = -1L // current "to-be-emitted" index, -1 means the slot is free now

    @JvmField
    var cont: Continuation<Unit>? = null // collector waiting for new value

    override fun allocateLocked(flow: SharedFlowImpl<*>): Boolean {
        if (index >= 0) return false // not free
        index = flow.updateNewCollectorIndexLocked()
        return true
    }

    override fun freeLocked(flow: SharedFlowImpl<*>): List<Continuation<Unit>>? {
        assert { index >= 0 }
        val oldIndex = index
        index = -1L
        cont = null // cleanup continuation reference
        return flow.updateCollectorIndexLocked(oldIndex)
    }
}

private class SharedFlowImpl<T>(
    private val replay: Int,
    private val bufferCapacity: Int,
    private val onBufferOverflow: BufferOverflow,
    private val initialValue: Any?
) : AbstractSharedFlow<SharedFlowSlot>(), MutableSharedFlow<T>, CancellableFlow<T>, FusibleFlow<T> {
    /*
        Logical structure of the buffer

                  buffered values
             /-----------------------\
                          replayCache      queued emitters
                          /----------\/----------------------\
         +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
         |   | 1 | 2 | 3 | 4 | 5 | 6 | E | E | E | E | E | E |   |   |   |
         +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
               ^           ^           ^                      ^
               |           |           |                      |
              head         |      head + bufferSize     head + totalSize
               |           |           |
     index of the slowest  |    index of the fastest
      possible collector   |     possible collector
               |           |
               |     replayIndex == new collector's index
               \---------------------- /
          range of possible minCollectorIndex

          head == minOf(minCollectorIndex, replayIndex) // by definition
          totalSize == bufferSize + queueSize // by definitiion

       INVARIANTS:
          minCollectorIndex = activeSlots.minOf { it.index } ?: (head + bufferSize)
          replayIndex <= head + bufferSize
     */

    // Stored state
    private var buffer: Array<Any?>? = null // allocated when needed, allocated size always power of two
    private var replayIndex = 0L // minimal index from which new collector gets values
    private var minCollectorIndex = 0L // minimal index of active collectors, equal to replayIndex if there are none
    private var bufferSize = 0 // number of buffered values
    private var queueSize = 0 // number of queued emitters

    // Computed state
    private val head: Long get() = minOf(minCollectorIndex, replayIndex)
    private val replaySize: Int get() = (head + bufferSize - replayIndex).toInt()
    private val totalSize: Int get() = bufferSize + queueSize
    private val bufferEndIndex: Long get() = head + bufferSize
    private val queueEndIndex: Long get() = head + bufferSize + queueSize

    init {
        if (initialValue !== NO_VALUE) {
            enqueueLocked(initialValue)
            bufferSize++
        }
    }

    override val replayCache: List<T>
        get() = synchronized(this) {
            val replaySize = this.replaySize
            if (replaySize == 0) return emptyList()
            val result = ArrayList<T>(replaySize)
            val buffer = buffer!! // must be allocated, because replaySize > 0
            @Suppress("UNCHECKED_CAST")
            for (i in 0 until replaySize) result += buffer.getBufferAt(replayIndex + i) as T
            result
        }

    @Suppress("UNCHECKED_CAST")
    override suspend fun collect(collector: FlowCollector<T>) {
        val slot = allocateSlot()
        try {
            if (collector is SubscribedFlowCollector) collector.onSubscription()
            val collectorJob = currentCoroutineContext()[Job]
            while (true) {
                var newValue: Any?
                while (true) {
                    newValue = tryTakeValue(slot) // attempt no-suspend fast path first
                    if (newValue !== NO_VALUE) break
                    awaitValue(slot) // await signal that the new value is available
                }
                collectorJob?.ensureActive()
                collector.emit(newValue as T)
            }
        } finally {
            freeSlot(slot)
        }
    }

    override fun tryEmit(value: T): Boolean {
        var resumeList: List<Continuation<Unit>>? = null
        val emitted = synchronized(this) {
            if (tryEmitLocked(value)) {
                resumeList = findSlotsToResumeLocked()
                true
            } else {
                false
            }
        }
        resumeList?.forEach { it.resume(Unit) }
        return emitted
    }

    override suspend fun emit(value: T) {
        if (tryEmit(value)) return // fast-path
        emitSuspend(value)
    }

    @Suppress("UNCHECKED_CAST")
    private fun tryEmitLocked(value: T): Boolean {
        // Fast path without collectors -> no buffering
        if (nCollectors == 0) return tryEmitNoCollectorsLocked(value) // always returns true
        // With collectors we'll have to buffer
        // cannot emit now if buffer is full && blocked by slow collectors
        if (bufferSize >= bufferCapacity && minCollectorIndex <= replayIndex) {
            when (onBufferOverflow) {
                BufferOverflow.SUSPEND -> return false // will suspend
                BufferOverflow.DROP_LATEST -> return true // just drop incoming
                BufferOverflow.DROP_OLDEST -> {} // force enqueue & drop oldest instead
            }
        }
        enqueueLocked(value)
        bufferSize++ // value was added to buffer
        // drop oldest from the buffer if it became more than bufferCapacity
        if (bufferSize > bufferCapacity) dropOldestLocked()
        // keep replaySize not larger that needed
        if (replaySize > replay) { // increment replayIndex by one
            updateBufferLocked(replayIndex + 1, minCollectorIndex, bufferEndIndex, queueEndIndex)
        }
        return true
    }

    private fun tryEmitNoCollectorsLocked(value: T): Boolean {
        assert { nCollectors == 0 }
        if (replay == 0) return true // no need to replay, just forget it now
        enqueueLocked(value) // enqueue to replayCache
        bufferSize++ // value was added to buffer
        // drop oldest from the buffer if it became more than replay
        if (bufferSize > replay) dropOldestLocked()
        minCollectorIndex = head + bufferSize // a default value (max allowed)
        return true
    }

    private fun dropOldestLocked() {
        buffer!!.setBufferAt(head, null)
        bufferSize--
        val newHead = head + 1
        if (replayIndex < newHead) replayIndex = newHead
        if (minCollectorIndex < newHead) correctCollectorIndexesOnDropOldest(newHead)
        assert { head == newHead } // since head = minOf(minCollectorIndex, replayIndex) it should have updated
    }

    private fun correctCollectorIndexesOnDropOldest(newHead: Long) {
        forEachSlotLocked { slot ->
            @Suppress("ConvertTwoComparisonsToRangeCheck") // Bug in JS backend
            if (slot.index >= 0 && slot.index < newHead) {
                slot.index = newHead // force move it up (this collector was too slow and missed the value at its index)
            }
        }
        minCollectorIndex = newHead
    }

    // enqueues item to buffer array, caller shall increment either bufferSize or queueSize
    private fun enqueueLocked(item: Any?) {
        val curSize = totalSize
        val buffer = when (val curBuffer = buffer) {
            null -> growBuffer(null, 0, 2)
            else -> if (curSize >= curBuffer.size) growBuffer(curBuffer, curSize,curBuffer.size * 2) else curBuffer
        }
        buffer.setBufferAt(head + curSize, item)
    }

    private fun growBuffer(curBuffer: Array<Any?>?, curSize: Int, newSize: Int): Array<Any?> {
        check(newSize > 0) { "Buffer size overflow" }
        val newBuffer = arrayOfNulls<Any?>(newSize).also { buffer = it }
        if (curBuffer == null) return newBuffer
        val head = head
        for (i in 0 until curSize) {
            newBuffer.setBufferAt(head + i, curBuffer.getBufferAt(head + i))
        }
        return newBuffer
    }

    private suspend fun emitSuspend(value: T) = suspendCancellableCoroutine<Unit> sc@{ cont ->
        var resumeList: List<Continuation<Unit>>? = null
        val emitter = synchronized(this) lock@{
            // recheck buffer under lock again (make sure it is really full)
            if (tryEmitLocked(value)) {
                cont.resume(Unit)
                resumeList = findSlotsToResumeLocked()
                return@lock null
            }
            // add suspended emitter to the buffer
            Emitter(this, head + totalSize, value, cont).also {
                enqueueLocked(it)
                queueSize++ // added to queue of waiting emitters
                // synchronous shared flow might rendezvous with waiting emitter
                if (bufferCapacity == 0) resumeList = findSlotsToResumeLocked()
            }
        }
        // outside of the lock: register dispose on cancellation
        emitter?.let { cont.disposeOnCancellation(it) }
        // outside of the lock: resume slots if needed
        resumeList?.forEach { it.resume(Unit) }
    }

    private fun cancelEmitter(emitter: Emitter) = synchronized(this) {
        if (emitter.index < head) return // already skipped past this index
        val buffer = buffer!!
        if (buffer.getBufferAt(emitter.index) !== emitter) return // already resumed
        buffer.setBufferAt(emitter.index, NO_VALUE)
        cleanupTailLocked()
    }

    internal fun updateNewCollectorIndexLocked(): Long {
        val index = replayIndex
        if (index < minCollectorIndex) minCollectorIndex = index
        return index
    }

    // Is called when collector disappears of changes index, returns a list of continuations to resume after lock
    internal fun updateCollectorIndexLocked(oldIndex: Long): List<Continuation<Unit>>? {
        assert { oldIndex >= minCollectorIndex }
        if (oldIndex > minCollectorIndex) return null // nothing changes, it was not min
        // start computing new minimal index of active collectors
        val head = head
        var newMinCollectorIndex = head + bufferSize
        // take into account a special case of sync shared flow that can go past 1st queued emitter
        if (bufferCapacity == 0 && queueSize > 0) newMinCollectorIndex++
        forEachSlotLocked { slot ->
            @Suppress("ConvertTwoComparisonsToRangeCheck") // Bug in JS backend
            if (slot.index >= 0 && slot.index < newMinCollectorIndex) newMinCollectorIndex = slot.index
        }
        assert { newMinCollectorIndex >= minCollectorIndex } // can only grow
        if (newMinCollectorIndex <= minCollectorIndex) return null // nothing changes
        // Compute new buffer size if we drop items we no longer need and no emitter is resumed:
        // We must keep all the items from newMinIndex to the end of buffer
        var newBufferEndIndex = bufferEndIndex // var to grow when waiters are resumed
        val maxResumeCount = if (nCollectors > 0) {
            // If we have collectors we can resume up to maxResumeCount waiting emitters
            // a) queueSize -> that's how many waiting emitters we have
            // b) bufferCapacity - newBufferSize0 -> that's how many we can afford to resume to add w/o exceeding bufferCapacity
            val newBufferSize0 = (newBufferEndIndex - newMinCollectorIndex).toInt()
            minOf(queueSize, bufferCapacity - newBufferSize0)
        } else {
            // If we don't have collectors anymore we must resume all waiting emitters
            queueSize // that's how many waiting emitters we have (at most)
        }
        var resumeList: ArrayList<Continuation<Unit>>? = null
        val newQueueEndIndex = newBufferEndIndex + queueSize
        if (maxResumeCount > 0) { // collect emitters to resume if we have them
            resumeList = ArrayList(maxResumeCount)
            val buffer = buffer!!
            for (curEmitterIndex in newBufferEndIndex until newQueueEndIndex) {
                val emitter = buffer.getBufferAt(curEmitterIndex)
                if (emitter !== NO_VALUE) {
                    emitter as Emitter // must have Emitter class
                    resumeList.add(emitter.cont)
                    buffer.setBufferAt(curEmitterIndex, NO_VALUE) // make as canceled if we moved ahead
                    buffer.setBufferAt(newBufferEndIndex, emitter.value)
                    newBufferEndIndex++
                    if (resumeList.size >= maxResumeCount) break // enough resumed, done
                }
            }
        }
        // Compute new buffer size -> how many values we now actually have after resume
        val newBufferSize1 = (newBufferEndIndex - head).toInt()
        // Compute new replay size -> limit to replay the number of items we need, take into account that it can only grow
        var newReplayIndex = maxOf(replayIndex, newBufferEndIndex - minOf(replay, newBufferSize1))
        // adjustment for synchronous case with cancelled emitter (NO_VALUE)
        if (bufferCapacity == 0 && newReplayIndex < newQueueEndIndex && buffer!!.getBufferAt(newReplayIndex) == NO_VALUE) {
            newBufferEndIndex++
            newReplayIndex++
        }
        // Update buffer state
        updateBufferLocked(newReplayIndex, newMinCollectorIndex, newBufferEndIndex, newQueueEndIndex)
        // just in case we've moved all buffered emitters and have NO_VALUE's at the tail now
        cleanupTailLocked()
        return resumeList
    }

    private fun updateBufferLocked(
        newReplayIndex: Long,
        newMinCollectorIndex: Long,
        newBufferEndIndex: Long,
        newQueueEndIndex: Long
    ) {
        // Compute new head value
        val newHead = minOf(newMinCollectorIndex, newReplayIndex)
        assert { newHead >= head }
        // cleanup items we don't have to buffer anymore (because head is about to move)
        for (index in head until newHead) buffer!!.setBufferAt(index, null)
        // update all state variable to newly computed values
        replayIndex = newReplayIndex
        minCollectorIndex = newMinCollectorIndex
        bufferSize = (newBufferEndIndex - newHead).toInt()
        queueSize = (newQueueEndIndex - newBufferEndIndex).toInt()
        // check our key invariants (just in case)
        assert { bufferSize >= 0 }
        assert { queueSize >= 0 }
        assert { replayIndex <= this.head + bufferSize }
    }

    // :todo:
    private fun cleanupTailLocked() {
        // If we have synchronous case, then keep one emitter queued
        if (bufferCapacity == 0 && queueSize <= 1) return // return, don't clear it
        val buffer = buffer!!
        while (queueSize > 0 && buffer.getBufferAt(head + totalSize - 1) === NO_VALUE) {
            queueSize--
            buffer.setBufferAt(head + totalSize, null)
        }
    }

    // returns NO_VALUE if cannot take value without suspension
    private fun tryTakeValue(slot: SharedFlowSlot): Any? {
        var resumeList: List<Continuation<Unit>>? = null
        val value = synchronized(this) {
            val index = tryPeekLocked(slot)
            if (index < 0) {
                NO_VALUE
            } else {
                val oldIndex = slot.index
                val newValue = getPeekedValueLockedAt(index)
                slot.index = index + 1 // points to the next index after peeked one
                resumeList = updateCollectorIndexLocked(oldIndex)
                newValue
            }
        }
        resumeList?.forEach { it.resume(Unit) }
        return value
    }

    // returns -1 if cannot peek value without suspension
    private fun tryPeekLocked(slot: SharedFlowSlot): Long {
        // return buffered value is possible
        val index = slot.index
        if (index < bufferEndIndex) return index
        if (bufferCapacity > 0) return -1L // if there's a buffer, never try to rendezvous with emitters
        // Synchronous shared flow (bufferCapacity == 0) tries to rendezvous
        if (index > head) return -1L // ... but only with the first emitter (never look forward)
        if (queueSize == 0) return -1L // nothing there to rendezvous with
        return index // rendezvous with the first emitter
    }

    private fun getPeekedValueLockedAt(index: Long): Any? =
        when (val item = buffer!!.getBufferAt(index)) {
            is Emitter -> item.value
            else -> item
        }

    private suspend fun awaitValue(slot: SharedFlowSlot): Unit = suspendCancellableCoroutine { cont ->
        synchronized(this) lock@{
            val index = tryPeekLocked(slot) // recheck under this lock
            if (index < 0) {
                slot.cont = cont // Ok -- suspending
            } else {
                cont.resume(Unit) // has value, no need to suspend
                return@lock
            }
            slot.cont = cont // suspend, waiting
        }
    }

    private fun findSlotsToResumeLocked(): List<Continuation<Unit>>? {
        var result: ArrayList<Continuation<Unit>>? = null
        forEachSlotLocked loop@{ slot ->
            val cont = slot.cont ?: return@loop // only waiting slots
            if (tryPeekLocked(slot) < 0) return@loop // only slots that can peek a value
            val a = result ?: ArrayList<Continuation<Unit>>(2).also { result = it }
            a.add(cont)
            slot.cont = null // not waiting anymore
        }
        return result
    }

    override fun createSlot() = SharedFlowSlot()
    override fun createSlotArray(size: Int): Array<SharedFlowSlot?> = arrayOfNulls(size)

    override fun resetReplayCache() {
        val resumeList= synchronized(this) {
            resetReplayCacheLocked()
            // resetting replay buffer could have resumed some collectors
            findSlotsToResumeLocked()
        }
        resumeList?.forEach { it.resume(Unit) }
    }

    private fun resetReplayCacheLocked() {
        var newMinCollectorIndex = minCollectorIndex
        var newBufferEndIndex = bufferEndIndex
        var newQueueEndIndex = queueEndIndex
        // enqueue initial value at the end if needed & compute newReplayIndex
        val newReplayIndex: Long
        if (initialValue === NO_VALUE) {
            // Check if replayCache was already reset and is at the right state...
            if (replaySize == 0) return // right state -> bail out
            newReplayIndex = newBufferEndIndex // otherwise correct replayIndex
        } else {
            assert { replay > 0 } // only supporting initial value with replay
            // Check if replayCache was already reset and is at the right state...
            val hasInitialValueInReplay = replaySize > 0 &&
                buffer!!.getBufferAt(newBufferEndIndex - 1) === initialValue
            if (replaySize == 1 && hasInitialValueInReplay) return // bail out if everything is fine
            if (hasInitialValueInReplay) {
                // it is there, but replay size is wrong
                newReplayIndex = newBufferEndIndex - 1 // correct replayIndex
            } else {
                enqueueLocked(initialValue) // Enqueue initialValue (grows buffer if needed)
                if (queueSize > 0) { // If we had non-empty queue of emitters, then
                    // ...  move newly enqueued item at the end of the buffer by moving all queued items to the right
                    val buffer = buffer!!
                    for (i in queueSize downTo 1) {
                        val item = buffer.getBufferAt(newBufferEndIndex + i - 1)
                        if (item is Emitter) item.index = newBufferEndIndex + i // correct its index
                        buffer.setBufferAt(newBufferEndIndex + i, item)
                    }
                    buffer.setBufferAt(newBufferEndIndex, initialValue)
                }
                // If "emission" of initialValue overflow buffer and it is dropping oldest value, then do drop oldest
                if (bufferSize >= bufferCapacity && onBufferOverflow == BufferOverflow.DROP_OLDEST) {
                    assert { bufferCapacity > 0 } // can only happen when there is a buffer
                    val newHead = head + 1 // drop one from the head of the buffer
                    if (minCollectorIndex < newHead) {
                        forEachSlotLocked { slot ->
                            @Suppress("ConvertTwoComparisonsToRangeCheck") // Bug in JS backend
                            if (slot.index >= 0 && slot.index < newHead) slot.index = newHead
                        }
                        newMinCollectorIndex = newHead
                    }
                }
                newQueueEndIndex++
                newReplayIndex = newBufferEndIndex++ // newReplayIndex is where we have initialValue now
            }
        }
        assert { newReplayIndex >= replayIndex }
        // Update buffer state
        updateBufferLocked(newReplayIndex, newMinCollectorIndex, newBufferEndIndex, newQueueEndIndex)
    }

    override fun fuse(context: CoroutineContext, capacity: Int, onBufferOverflow: BufferOverflow) =
        fuseSharedFlow(context, capacity, onBufferOverflow)
    
    private class Emitter(
        @JvmField val flow: SharedFlowImpl<*>,
        @JvmField var index: Long,
        @JvmField val value: Any?,
        @JvmField val cont: Continuation<Unit>
    ) : DisposableHandle {
        override fun dispose() = flow.cancelEmitter(this)
    }
}

@SharedImmutable
@JvmField
internal val NO_VALUE = Symbol("NO_VALUE")

private fun Array<Any?>.getBufferAt(index: Long) = get(index.toInt() and (size - 1))
private fun Array<Any?>.setBufferAt(index: Long, item: Any?) = set(index.toInt() and (size - 1), item)

internal fun <T> SharedFlow<T>.fuseSharedFlow(
    context: CoroutineContext,
    capacity: Int,
    onBufferOverflow: BufferOverflow
): Flow<T> {
    // context is irrelevant for shared flow and making additional rendezvous is meaningless
    // however, additional non-trivial buffering after shared flow could make sense for very slow subscribers
    if ((capacity == Channel.RENDEZVOUS || capacity == Channel.OPTIONAL_CHANNEL) && onBufferOverflow == BufferOverflow.SUSPEND) {
        return this
    }
    // Apply channel flow operator as usual
    return ChannelFlowOperatorImpl(this, context, capacity, onBufferOverflow)
}
