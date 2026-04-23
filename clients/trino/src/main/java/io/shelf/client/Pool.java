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

/**
 * The two Shelf cache pools per BLUEPRINT §6.1. The Rust daemon dispatches
 * on the path segment {@code /cache/<pool>/...}; the plugin must pick one
 * before issuing a request. The decision is made at the call site because
 * metadata reads (Iceberg JSON/manifests, Parquet footers) and rowgroup
 * reads have different residency expectations and admission thresholds.
 */
public enum Pool
{
    METADATA("metadata"),
    ROWGROUP("rowgroup");

    private final String wire;

    Pool(String wire)
    {
        this.wire = wire;
    }

    /** The exact string used in the {@code /cache/<pool>/...} URL path. */
    public String wire()
    {
        return wire;
    }
}
