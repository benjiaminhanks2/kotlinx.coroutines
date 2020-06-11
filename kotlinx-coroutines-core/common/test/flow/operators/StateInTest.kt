/*
 * Copyright 2016-2020 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.flow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlin.test.*

/**
 * It is mostly covered by [ShareInTest], this just add state-specific checks.
 */
class StateInTest : TestBase() {
    @Test
    fun testOperatorFusion() = runTest {
        val state = flowOf("OK").stateIn(this)
        assertTrue(state !is MutableStateFlow<*>) // cannot be cast to mutable state flow
        assertSame(state, (state as Flow<*>).cancellable())
        assertSame(state, (state as Flow<*>).distinctUntilChanged())
        assertSame(state, (state as Flow<*>).flowOn(Dispatchers.Default))
        assertSame(state, (state as Flow<*>).conflate())
        assertSame(state, state.buffer(Channel.CONFLATED))
        assertSame(state, state.buffer(Channel.RENDEZVOUS))
        assertSame(state, state.buffer(onBufferOverflow = BufferOverflow.DROP_OLDEST))
        assertSame(state, state.buffer(0, onBufferOverflow = BufferOverflow.DROP_OLDEST))
        assertSame(state, state.buffer(1, onBufferOverflow = BufferOverflow.DROP_OLDEST))
        coroutineContext.cancelChildren()
    }
}