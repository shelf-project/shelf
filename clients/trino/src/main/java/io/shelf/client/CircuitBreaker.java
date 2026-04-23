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

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Per-pod fail-open circuit breaker for Shelf.
 *
 * <p><b>Fail-open invariant.</b> This class encodes the exact state machine
 * specified in BLUEPRINT §9.5. Trino must <em>never</em> see a Shelf-specific
 * error: every transition ends in a decision to either (a) talk to Shelf or
 * (b) fall through to direct S3. This class never throws from a decision
 * method.
 *
 * <p>Semantics (verbatim from §9.5):
 * <ul>
 *   <li><b>Closed</b>: normal operation. Up to 5 <em>consecutive</em>
 *     failures before opening.</li>
 *   <li><b>Open</b>: for the next 10 s, bypass Shelf entirely for any key
 *     hashing to this pod. No retries.</li>
 *   <li><b>Half-open</b>: after the timer expires, exactly one in-flight
 *     probe is permitted. Success &rarr; closed. Failure &rarr; open with
 *     the timer doubled (exponential back-off, capped).</li>
 * </ul>
 *
 * <p>Instances are keyed by pod id in {@link HashRing}; one breaker per pod.
 *
 * <p><b>Thread safety.</b> All public methods are safe for concurrent
 * invocation from any Trino worker thread. The state machine is
 * implemented as a CAS loop over {@link State}; no lock is held across
 * a network call.
 */
public final class CircuitBreaker
{
    /** Failure threshold — see BLUEPRINT §9.5 (5 consecutive failures). */
    public static final int DEFAULT_FAILURE_THRESHOLD = 5;

    /** Initial open timer — see BLUEPRINT §9.5 (10 s). */
    public static final Duration DEFAULT_OPEN_DURATION = Duration.ofSeconds(10);

    /** Ceiling for the exponentially-doubled open timer. */
    public static final Duration DEFAULT_MAX_OPEN_DURATION = Duration.ofMinutes(5);

    /** Breaker state machine. */
    public enum State
    {
        CLOSED,
        OPEN,
        HALF_OPEN
    }

    private final String podId;
    private final int failureThreshold;
    private final long initialOpenDurationNanos;
    private final long maxOpenDurationNanos;
    private final Clock clock;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final AtomicLong openedAtNanos = new AtomicLong();
    private final AtomicLong currentOpenDurationNanos;
    /**
     * Monotonically increasing ticket. Each time we enter OPEN we bump
     * this. {@link #tryAcquireProbeToken()} in HALF_OPEN uses a CAS that
     * only succeeds once per OPEN episode, guaranteeing at most one probe.
     */
    private final AtomicInteger probeGeneration = new AtomicInteger();
    private final AtomicInteger probeClaim = new AtomicInteger(-1);

    public CircuitBreaker(String podId)
    {
        this(podId, DEFAULT_FAILURE_THRESHOLD, DEFAULT_OPEN_DURATION, DEFAULT_MAX_OPEN_DURATION, Clock.system());
    }

    public CircuitBreaker(
            String podId,
            int failureThreshold,
            Duration initialOpenDuration,
            Duration maxOpenDuration,
            Clock clock)
    {
        this.podId = Objects.requireNonNull(podId, "podId");
        if (failureThreshold < 1) {
            throw new IllegalArgumentException("failureThreshold must be >= 1");
        }
        Objects.requireNonNull(initialOpenDuration, "initialOpenDuration");
        Objects.requireNonNull(maxOpenDuration, "maxOpenDuration");
        if (initialOpenDuration.isNegative() || initialOpenDuration.isZero()) {
            throw new IllegalArgumentException("initialOpenDuration must be positive");
        }
        if (maxOpenDuration.compareTo(initialOpenDuration) < 0) {
            throw new IllegalArgumentException("maxOpenDuration must be >= initialOpenDuration");
        }
        this.failureThreshold = failureThreshold;
        this.initialOpenDurationNanos = initialOpenDuration.toNanos();
        this.maxOpenDurationNanos = maxOpenDuration.toNanos();
        this.clock = Objects.requireNonNull(clock, "clock");
        this.currentOpenDurationNanos = new AtomicLong(this.initialOpenDurationNanos);
    }

    public String podId()
    {
        return podId;
    }

    /**
     * Current observed state. Note this is a <em>snapshot</em>; the
     * breaker may transition between the read and the next call. Use
     * {@link #isOpen()} to drive actual routing decisions.
     */
    public State state()
    {
        maybeExpireOpenTimer();
        return state.get();
    }

    /**
     * Decide whether the next call should short-circuit to direct S3.
     *
     * @return {@code true} iff the breaker is OPEN and the open timer
     *         has not yet elapsed. Returns {@code false} in CLOSED and
     *         HALF_OPEN: in HALF_OPEN exactly one caller (the holder of
     *         {@link #tryAcquireProbeToken()}) should actually call
     *         Shelf; others should fall through.
     */
    public boolean isOpen()
    {
        maybeExpireOpenTimer();
        return state.get() == State.OPEN;
    }

    /**
     * Attempt to acquire the single probe slot in HALF_OPEN.
     *
     * <p>Returns {@code true} for exactly one caller per OPEN episode.
     * All other callers during the HALF_OPEN window see {@code false}
     * and must fall through to direct S3.
     */
    public boolean tryAcquireProbeToken()
    {
        maybeExpireOpenTimer();
        if (state.get() != State.HALF_OPEN) {
            return false;
        }
        int gen = probeGeneration.get();
        // Winner is the first thread to CAS probeClaim from a value
        // other than `gen` to `gen`.
        int prev = probeClaim.get();
        if (prev == gen) {
            return false;
        }
        return probeClaim.compareAndSet(prev, gen);
    }

    /**
     * Record a Shelf-originated success. Transitions HALF_OPEN &rarr;
     * CLOSED, resets failure count and back-off timer.
     */
    public void recordSuccess()
    {
        consecutiveFailures.set(0);
        currentOpenDurationNanos.set(initialOpenDurationNanos);
        state.set(State.CLOSED);
    }

    /**
     * Record a Shelf-originated failure.
     *
     * <p>Only the following exception types are {@link CircuitBreaker}
     * failures: {@code IOException}, {@code TimeoutException},
     * {@code ConnectException}, HTTP 503, HTTP 504. Real S3 errors
     * (AccessDenied, NoSuchKey) bubble up unchanged.
     *
     * <p>From CLOSED: increment counter; trip to OPEN when the counter
     * reaches {@link #failureThreshold}. From HALF_OPEN: trip back to
     * OPEN immediately and double the open timer (capped at
     * {@link #DEFAULT_MAX_OPEN_DURATION}).
     */
    public void recordFailure()
    {
        maybeExpireOpenTimer();
        State current = state.get();
        if (current == State.HALF_OPEN) {
            // Probe failed — back off harder.
            long next = Math.min(currentOpenDurationNanos.get() * 2L, maxOpenDurationNanos);
            // Guard against overflow on pathological durations.
            if (next < 0) {
                next = maxOpenDurationNanos;
            }
            currentOpenDurationNanos.set(next);
            tripOpen();
            return;
        }
        if (current == State.OPEN) {
            // Already open — a stray failure in this window is ignored.
            return;
        }
        // CLOSED: count consecutive failures.
        if (consecutiveFailures.incrementAndGet() >= failureThreshold) {
            tripOpen();
        }
    }

    private void tripOpen()
    {
        openedAtNanos.set(clock.nanoTime());
        probeGeneration.incrementAndGet();
        state.set(State.OPEN);
    }

    private void maybeExpireOpenTimer()
    {
        if (state.get() != State.OPEN) {
            return;
        }
        long elapsed = clock.nanoTime() - openedAtNanos.get();
        if (elapsed >= currentOpenDurationNanos.get()) {
            state.compareAndSet(State.OPEN, State.HALF_OPEN);
        }
    }

    /** Testable clock seam. Production uses {@link Clock#system()}. */
    public interface Clock
    {
        long nanoTime();

        static Clock system()
        {
            return System::nanoTime;
        }
    }
}
