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
 *     probe is permitted. Success → closed. Failure → open with the timer
 *     doubled (exponential back-off, capped).</li>
 * </ul>
 *
 * <p>Instances are keyed by pod id in {@link HashRing}; one breaker per pod.
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
    private final Duration initialOpenDuration;
    private final Duration maxOpenDuration;
    private final Clock clock;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger consecutiveFailures = new AtomicInteger();
    private final AtomicLong openedAtNanos = new AtomicLong();
    private final AtomicLong currentOpenDurationNanos;
    private final AtomicInteger halfOpenProbeToken = new AtomicInteger();

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
        this.failureThreshold = failureThreshold;
        this.initialOpenDuration = Objects.requireNonNull(initialOpenDuration, "initialOpenDuration");
        this.maxOpenDuration = Objects.requireNonNull(maxOpenDuration, "maxOpenDuration");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.currentOpenDurationNanos = new AtomicLong(initialOpenDuration.toNanos());
    }

    public String podId()
    {
        return podId;
    }

    public State state()
    {
        return state.get();
    }

    /**
     * Decide whether the next call should short-circuit to direct S3.
     *
     * @return {@code true} if the breaker is OPEN and not yet ready to probe.
     */
    public boolean isOpen()
    {
        // TODO(SHELF-11): implement timer expiry + CLOSED→OPEN→HALF_OPEN transitions
        //   per BLUEPRINT §9.5. Must never throw; must be safe to call from
        //   multiple Trino worker threads concurrently (see class javadoc).
        return false;
    }

    /**
     * Check out a half-open probe token. Exactly one caller wins; every other
     * concurrent caller sees the breaker as OPEN.
     */
    public boolean tryAcquireProbeToken()
    {
        // TODO(SHELF-11): single-probe contention test per 03-plan.md §4 SHELF-11.
        return false;
    }

    /** Record a Shelf-originated success. Transitions half-open → closed. */
    public void recordSuccess()
    {
        // TODO(SHELF-11): reset consecutiveFailures, reset open timer, state=CLOSED.
    }

    /**
     * Record a Shelf-originated failure.
     *
     * <p>Only the following exception types are {@link CircuitBreaker} failures:
     * {@code IOException}, {@code TimeoutException}, {@code ConnectException},
     * HTTP 503, HTTP 504. Real S3 errors (AccessDenied, NoSuchKey) bubble up
     * unchanged.
     */
    public void recordFailure()
    {
        // TODO(SHELF-11): 5-failure threshold → OPEN; HALF_OPEN failure → OPEN
        //   with currentOpenDurationNanos = min(current * 2, maxOpenDuration).
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
