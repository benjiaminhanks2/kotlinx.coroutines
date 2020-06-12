/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.channels

import kotlinx.coroutines.*

/**
 * A strategy for buffer overflow handling in [channels][Channel] and [flows][kotlinx.coroutines.flow.Flow] that
 * controls what is going to be sacrificed on buffer overflow:
 *
 * * [SUSPEND] &mdash; upstream that is [sending][SendChannel.send] or
 *   is [emitting][kotlinx.coroutines.flow.FlowCollector.emit] a value is **suspended** while the buffer is full.
 * * [DROP_OLDEST] &mdash; drop **the oldest** value in the buffer on overflow, keep the latest value, do not suspend.
 * * [DROP_LATEST] &mdash; drop **the latest** value in the buffer on overflow, keep the oldest value, do not suspend.
 */
@ExperimentalCoroutinesApi
public enum class BufferOverflow {
    /**
     * Suspend on buffer overflow.
     */
    SUSPEND,

    /**
     * Drop **the oldest** value in the buffer on overflow, keep the latest value, do not suspend.
     */
    DROP_OLDEST,

    /**
     * Drop **the latest** value in the buffer on overflow, keep the oldest value, do not suspend.
     */
    DROP_LATEST
}
