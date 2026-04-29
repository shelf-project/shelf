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
package io.shelf.listener.config;

import java.util.Locale;

/**
 * Behaviour when the bounded ingest queue is full at {@code queryCompleted}
 * time. The default is {@link #DROP} — the only mode that can never delay
 * Trino's coordinator thread, and therefore the only safe production
 * default per the SHELF-37 acceptance criteria.
 *
 * <p>Mapping from {@code shelf.listener.fail-mode}:
 * <ul>
 *   <li>{@code drop} → {@link #DROP}: enqueue with timeout 0, fail-fast,
 *       increment {@code shelf_listener_dropped_total{reason="queue_full"}}.
 *   <li>{@code block} → {@link #BLOCK}: enqueue with the configured block
 *       timeout. After the timeout elapses fall through to the same drop
 *       counter so the metric still reflects the loss.
 *   <li>{@code log_only} → {@link #LOG_ONLY}: short-circuits before the
 *       queue. Never writes. Logs a WARN every N events. Use during dry
 *       runs or for a kill-switch identical in shape to {@code write.enabled=false}.
 * </ul>
 */
public enum FailMode
{
    DROP,
    BLOCK,
    LOG_ONLY;

    public static FailMode parse(String raw)
    {
        if (raw == null) {
            return DROP;
        }
        switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "drop":
                return DROP;
            case "block":
                return BLOCK;
            case "log_only":
            case "log-only":
            case "logonly":
                return LOG_ONLY;
            default:
                throw new IllegalArgumentException(
                        "shelf.listener.fail-mode must be one of {drop, block, log_only}; got: " + raw);
        }
    }
}
