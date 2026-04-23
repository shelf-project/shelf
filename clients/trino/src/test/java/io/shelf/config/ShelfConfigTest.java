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
package io.shelf.config;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShelfConfigTest
{
    @Test
    void defaultsWhenMapIsEmpty()
    {
        ShelfConfig cfg = ShelfConfig.fromMap(Map.of());
        assertThat(cfg.isEnabled()).isFalse();
        assertThat(cfg.getEndpoint()).isEqualTo(ShelfConfig.DEFAULT_ENDPOINT);
        assertThat(cfg.getTenant()).isEqualTo(ShelfConfig.DEFAULT_TENANT);
        assertThat(cfg.getFallbackOnError()).isEqualTo("direct-s3");
        assertThat(cfg.isPrefetchEnabled()).isFalse();
        assertThat(cfg.getGranularity()).containsExactlyInAnyOrder("row-group", "footer", "manifest");
        assertThat(cfg.getRpcTimeout()).isEqualTo(ShelfConfig.DEFAULT_RPC_TIMEOUT);
    }

    @Test
    void parsesFullValidMap()
    {
        ShelfConfig cfg = ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENABLED, "true",
                ShelfConfig.KEY_ENDPOINT, "shelf.shelf.svc.cluster.local:9090",
                ShelfConfig.KEY_TENANT, "replica-2",
                ShelfConfig.KEY_FALLBACK_ON_ERROR, "direct-s3",
                ShelfConfig.KEY_PREFETCH_ENABLED, "true",
                ShelfConfig.KEY_GRANULARITY, "row-group, footer",
                ShelfConfig.KEY_RPC_TIMEOUT_MS, "150"));
        assertThat(cfg.isEnabled()).isTrue();
        assertThat(cfg.getTenant()).isEqualTo("replica-2");
        assertThat(cfg.isPrefetchEnabled()).isTrue();
        assertThat(cfg.getGranularity()).containsExactlyInAnyOrder("row-group", "footer");
        assertThat(cfg.getRpcTimeout()).isEqualTo(Duration.ofMillis(150));
    }

    @Test
    void rejectsUnknownShelfKey()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of("shelf.mystery", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shelf.mystery");
    }

    @Test
    void ignoresNonShelfKeys()
    {
        ShelfConfig cfg = ShelfConfig.fromMap(Map.of(
                "fs.shelf.enabled", "true",
                "hive.metastore.uri", "thrift://x:9083"));
        assertThat(cfg.isEnabled()).isFalse();
    }

    @Test
    void rejectsInvalidFallbackOnErrorValue()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_FALLBACK_ON_ERROR, "fail-closed")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shelf.fallback.on-error")
                .hasMessageContaining("direct-s3");
    }

    @Test
    void rejectsEndpointWithoutPort()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENDPOINT, "shelf.local")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("host:port");
    }

    @Test
    void rejectsEndpointPortOutOfRange()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENDPOINT, "shelf.local:99999")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("out of range");
    }

    @Test
    void rejectsNonBooleanEnabled()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_ENABLED, "yes")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("true")
                .hasMessageContaining("false");
    }

    @Test
    void rejectsIllegalGranularityToken()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_GRANULARITY, "row-group,page")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("page");
    }

    @Test
    void rejectsEmptyGranularity()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_GRANULARITY, "  , ,")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonPositiveRpcTimeout()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_RPC_TIMEOUT_MS, "0")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNonNumericRpcTimeout()
    {
        assertThatThrownBy(() -> ShelfConfig.fromMap(Map.of(
                ShelfConfig.KEY_RPC_TIMEOUT_MS, "fast")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
