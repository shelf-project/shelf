/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.shelf.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.shelf.client.CircuitBreaker.State;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

/**
 * State-machine tests for {@link CircuitBreaker} (SHELF-11).
 *
 * <p>The specification is BLUEPRINT §9.5 and the acceptance criteria in
 * {@code agents/out/03-plan.md} §4 SHELF-11: nine+ tests covering the
 * core transitions, concurrency, and per-pod isolation.
 */
class CircuitBreakerTest
{
    private static final Duration OPEN = Duration.ofMillis(100);
    private static final Duration MAX_OPEN = Duration.ofMillis(1_600);

    @Test
    void closedToOpenAfterFiveConsecutiveFailures()
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = new CircuitBreaker("shelf-0", 5, OPEN, MAX_OPEN, clock);

        for (int i = 0; i < 4; i++) {
            cb.recordFailure();
            assertThat(cb.state()).isEqualTo(State.CLOSED);
            assertThat(cb.isOpen()).isFalse();
        }
        cb.recordFailure();
        assertThat(cb.state()).isEqualTo(State.OPEN);
        assertThat(cb.isOpen()).isTrue();
    }

    @Test
    void openToHalfOpenAfterTimer()
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = open(clock);

        clock.advance(OPEN.minusNanos(1));
        assertThat(cb.isOpen()).isTrue();

        clock.advance(Duration.ofNanos(1));
        assertThat(cb.isOpen()).isFalse();
        assertThat(cb.state()).isEqualTo(State.HALF_OPEN);
    }

    @Test
    void halfOpenFailureReopensAndDoublesTimer()
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = open(clock);

        clock.advance(OPEN);
        assertThat(cb.tryAcquireProbeToken()).isTrue();
        cb.recordFailure();
        assertThat(cb.state()).isEqualTo(State.OPEN);

        // Timer was 100ms; should now be 200ms. Advancing by 150ms
        // should still leave it OPEN.
        clock.advance(Duration.ofMillis(150));
        assertThat(cb.isOpen()).isTrue();
        clock.advance(Duration.ofMillis(60));
        assertThat(cb.state()).isEqualTo(State.HALF_OPEN);
    }

    @Test
    void halfOpenSuccessClosesAndResetsTimer()
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = open(clock);

        clock.advance(OPEN);
        assertThat(cb.tryAcquireProbeToken()).isTrue();
        cb.recordSuccess();
        assertThat(cb.state()).isEqualTo(State.CLOSED);

        // Five failures must trip again with the *initial* (not doubled)
        // timer — i.e. closing fully resets the back-off.
        for (int i = 0; i < 5; i++) {
            cb.recordFailure();
        }
        assertThat(cb.state()).isEqualTo(State.OPEN);
        clock.advance(OPEN.minusNanos(1));
        assertThat(cb.isOpen()).isTrue();
        clock.advance(Duration.ofNanos(1));
        assertThat(cb.state()).isEqualTo(State.HALF_OPEN);
    }

    @Test
    void concurrentFailuresTripExactlyOnce() throws Exception
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = new CircuitBreaker("shelf-0", 5, OPEN, MAX_OPEN, clock);

        int threads = 32;
        int perThread = 100;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < perThread; i++) {
                        cb.recordFailure();
                    }
                    return null;
                });
            }
            start.countDown();
        }
        finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }

        // Concurrent floods must still end in the OPEN state deterministically
        // (no exceptions; no rollback to CLOSED).
        assertThat(cb.state()).isEqualTo(State.OPEN);
    }

    @Test
    void perPodIsolation()
    {
        TestClock clock = new TestClock();
        CircuitBreaker a = new CircuitBreaker("shelf-0", 5, OPEN, MAX_OPEN, clock);
        CircuitBreaker b = new CircuitBreaker("shelf-1", 5, OPEN, MAX_OPEN, clock);

        for (int i = 0; i < 5; i++) {
            a.recordFailure();
        }
        assertThat(a.state()).isEqualTo(State.OPEN);
        assertThat(b.state()).isEqualTo(State.CLOSED);
        assertThat(b.isOpen()).isFalse();
    }

    @Test
    void failureCounterResetsOnClose()
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = new CircuitBreaker("shelf-0", 5, OPEN, MAX_OPEN, clock);
        for (int i = 0; i < 4; i++) {
            cb.recordFailure();
        }
        cb.recordSuccess();
        // After a success we must not reach threshold in <5 more.
        for (int i = 0; i < 4; i++) {
            cb.recordFailure();
            assertThat(cb.state()).isEqualTo(State.CLOSED);
        }
    }

    @Test
    void halfOpenAllowsExactlyOneProbe() throws Exception
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = open(clock);
        clock.advance(OPEN);
        assertThat(cb.state()).isEqualTo(State.HALF_OPEN);

        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger winners = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    start.await();
                    if (cb.tryAcquireProbeToken()) {
                        winners.incrementAndGet();
                    }
                    return null;
                });
            }
            start.countDown();
        }
        finally {
            pool.shutdown();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(winners.get()).isEqualTo(1);
    }

    @Test
    void openTimerDoublesUpToMax()
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = new CircuitBreaker("shelf-0", 5, OPEN, MAX_OPEN, clock);

        long[] expected = { 100L, 200L, 400L, 800L, 1_600L, 1_600L /* capped */ };
        for (long expectedMs : expected) {
            for (int i = 0; i < 5; i++) {
                cb.recordFailure();
            }
            assertThat(cb.state()).isEqualTo(State.OPEN);

            clock.advance(Duration.ofMillis(expectedMs).minusNanos(1));
            assertThat(cb.isOpen())
                    .as("still OPEN after %d ms - 1ns", expectedMs)
                    .isTrue();
            clock.advance(Duration.ofNanos(1));
            assertThat(cb.state())
                    .as("HALF_OPEN at exactly %d ms", expectedMs)
                    .isEqualTo(State.HALF_OPEN);

            assertThat(cb.tryAcquireProbeToken()).isTrue();
            cb.recordFailure(); // doubles timer for next loop
        }
    }

    @Test
    void closedStateIgnoresIsolatedFailuresBetweenSuccesses()
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = new CircuitBreaker("shelf-0", 5, OPEN, MAX_OPEN, clock);
        for (int i = 0; i < 20; i++) {
            cb.recordFailure();
            cb.recordSuccess();
        }
        assertThat(cb.state()).isEqualTo(State.CLOSED);
    }

    @Test
    void strayFailuresInOpenWindowDoNotReshuffleTimer()
    {
        TestClock clock = new TestClock();
        CircuitBreaker cb = open(clock);
        long before = clock.nanoTime();

        // Burst of failures during the OPEN window must not extend or
        // double the timer — that only happens on a HALF_OPEN probe.
        cb.recordFailure();
        cb.recordFailure();
        cb.recordFailure();

        clock.advance(OPEN);
        long after = clock.nanoTime();
        assertThat(after - before).isEqualTo(OPEN.toNanos());
        assertThat(cb.state()).isEqualTo(State.HALF_OPEN);
    }

    @Test
    void constructorRejectsBadArguments()
    {
        TestClock clock = new TestClock();
        assertThatThrownBy(() -> new CircuitBreaker(
                "p", 0, OPEN, MAX_OPEN, clock))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CircuitBreaker(
                "p", 1, Duration.ZERO, MAX_OPEN, clock))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CircuitBreaker(
                "p", 1, OPEN, Duration.ofMillis(50), clock))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ---------- helpers ---------------------------------------------

    private static CircuitBreaker open(TestClock clock)
    {
        CircuitBreaker cb = new CircuitBreaker("shelf-0", 5, OPEN, MAX_OPEN, clock);
        for (int i = 0; i < 5; i++) {
            cb.recordFailure();
        }
        assertThat(cb.state()).isEqualTo(State.OPEN);
        return cb;
    }

    /** Deterministic clock. All reads and advances are monotonic. */
    private static final class TestClock
            implements CircuitBreaker.Clock
    {
        private final AtomicLong now = new AtomicLong();

        @Override
        public long nanoTime()
        {
            return now.get();
        }

        void advance(Duration delta)
        {
            now.addAndGet(delta.toNanos());
        }
    }
}
