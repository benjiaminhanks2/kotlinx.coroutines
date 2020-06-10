/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.internal.*

/**
 * A command emitted by [SharingStarted] implementation to control the sharing coroutine in
 * [shareIn] and [stateIn] operators.
 */
@ExperimentalCoroutinesApi
public enum class SharingCommand {
    /**
     * Start the sharing coroutine.
     */
    START,

    /**
     * Stop the sharing coroutine.
     */
    STOP,

    /**
     * Stop the sharing coroutine and [reset buffer][MutableSharedFlow.resetBuffer] of the shared flow.
     */
    STOP_AND_RESET_BUFFER
}

/**
 * A strategy for starting and stopping sharing coroutine in [shareIn] and [stateIn] operators.
 *
 * This interface provides a set of built-in strategies: [Eagerly], [Lazily], [WhileSubscribed], and
 * supports custom strategies by implementing this interface's [command] function.
 *
 * For example, it is possible to define a custom strategy that starts upstream only when the number
 * of subscribers exceeds the given `threshold` and make it an extension on [SharingStarted.Companion] so
 * that it looks like a built-in strategy on the use-site:
 *
 * ```
 * fun SharingStarted.Companion.WhileSubscribedAtLeast(threshold: Int): SharingStarted =
 *     object : SharingStarted {
 *         override fun command(subscriptionCount: StateFlow<Int>): Flow<SharingCommand> =
 *             subscriptionCount
 *                 .map { if (it >= threshold) SharingCommand.START else SharingCommand.STOP }
 *     }
 * ```
 */
@ExperimentalCoroutinesApi
public interface SharingStarted {
    public companion object {
        /**
         * Sharing is started immediately and never stops.
         */
        @ExperimentalCoroutinesApi
        public val Eagerly: SharingStarted = StartedEagerly() // always init because it is a default, likely needed

        /**
         * Sharing is started when the first subscriber appears and never stops.
         */
        @ExperimentalCoroutinesApi
        public val Lazily: SharingStarted by lazy { StartedLazily() }

        /**
         * Sharing is started when the first subscriber appears, immediately stops when the last
         * subscriber disappears (by default), keeping the replay cache forever (by default).
         *
         * It has the following optional parameters:
         *
         * * [stopTimeoutMillis] &mdash; configures a delay (in milliseconds) between disappearance of the last
         *   subscriber and stop of the sharing coroutine. It defaults to zero (stop immediately).
         * * [replayExpirationMillis] &mdash; configures a delay (in milliseconds) between stop of
         *   the sharing coroutine and [buffer reset][MutableSharedFlow.resetBuffer].
         *   It defaults to `Long.MAX_VALUE` (keep replay cache forever, never reset buffer)
         *
         * This function throws [IllegalArgumentException] when either [stopTimeoutMillis] or [replayExpirationMillis]
         * are negative.
         */
        @Suppress("FunctionName")
        @ExperimentalCoroutinesApi
        public fun WhileSubscribed(stopTimeoutMillis: Long = 0, replayExpirationMillis: Long = Long.MAX_VALUE): SharingStarted =
            StartedWhileSubscribed(stopTimeoutMillis, replayExpirationMillis)
    }

    /**
     * Transforms the [subscriptionCount][MutableSharedFlow.subscriptionCount] state of the shared flow into the
     * flow of [commands][SharingCommand] that control sharing coroutine.
     */
    public fun command(subscriptionCount: StateFlow<Int>): Flow<SharingCommand>
}

// -------------------------------- implementation --------------------------------

private class StartedEagerly : SharingStarted {
    private val alwaysStarted = unsafeDistinctFlow { emit(SharingCommand.START) }
    override fun command(subscriptionCount: StateFlow<Int>): Flow<SharingCommand> = alwaysStarted
    override fun toString(): String = "SharingStarted.Eagerly"
}

private class StartedLazily : SharingStarted {
    override fun command(subscriptionCount: StateFlow<Int>): Flow<SharingCommand> = unsafeDistinctFlow {
        var started = false
        subscriptionCount.collect { count ->
            if (count > 0 && !started) {
                started = true
                emit(SharingCommand.START)
            }
        }
    }

    override fun toString(): String = "SharingStarted.Lazily"
}

private class StartedWhileSubscribed(
    private val stopTimeout: Long,
    private val replayExpiration: Long
) : SharingStarted {
    init {
        require(stopTimeout >= 0) { "stopTimeout($stopTimeout) cannot be negative" }
        require(replayExpiration >= 0) { "replayExpiration($replayExpiration) cannot be negative" }
    }

    override fun command(subscriptionCount: StateFlow<Int>): Flow<SharingCommand> = subscriptionCount
        .transformLatest { count ->
            if (count > 0) {
                emit(SharingCommand.START)
            } else {
                delay(stopTimeout)
                if (replayExpiration > 0) {
                    emit(SharingCommand.STOP)
                    delay(replayExpiration)
                }
                emit(SharingCommand.STOP_AND_RESET_BUFFER)
            }
        }
        .dropWhile { it != SharingCommand.START } // don't emit any STOP/RESET_BUFFER to start with, only START
        .distinctUntilChanged() // just in case somebody forgets it, don't leak our multiple sending of START

    @OptIn(ExperimentalStdlibApi::class)
    override fun toString(): String {
        val params = buildList(2) {
            if (stopTimeout > 0) add("stopTimeout=${stopTimeout}ms")
            if (replayExpiration < Long.MAX_VALUE) add("replayExpiration=${replayExpiration}ms")
        }
        return "SharingStarted.whileSubscribed(${params.joinToString()})"
    }
}
