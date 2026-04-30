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
 * Minimal seam for Shelf range fetches, decoupled from the concrete HTTP
 * client. Production uses {@link ShelfHttpClient}; tests inject fakes that
 * always succeed, always fail, or fail only the first N calls.
 */
@FunctionalInterface
public interface RangeFetcher
{
    byte[] rangeGet(String endpoint, Pool pool, String contentKey, long offset, long length)
            throws ShelfHttpClient.ShelfUnavailableException;

    static RangeFetcher of(ShelfHttpClient client)
    {
        return client::rangeGet;
    }
}
