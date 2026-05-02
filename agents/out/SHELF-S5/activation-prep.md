# S5 — ShelfFileSystem in-process activation (BLOCKED on upstream Trino SPI)

## Status

- Upstream PR: aamir306/trino:shelf/fs-spi-hook (commit 9d68b98) — staged, not submitted (JDK 25 needed)
- Once upstream merges, this doc becomes the activation patch

## Pre-staged changes

When `Plugin.getFileSystemFactories()` lands in trino-spi:

### Patch 1: clients/trino/src/main/java/io/shelf/plugin/ShelfPlugin.java

```java
// ADD after existing getEventListenerFactories() override
@Override
public Iterable<TrinoFileSystemFactory> getFileSystemFactories()
{
    return List.of(new ShelfTrinoFileSystemFactory());
}

// NEW nested class — wraps existing buildFileSystemFactory() seam
public static final class ShelfTrinoFileSystemFactory
        implements io.trino.spi.filesystem.TrinoFileSystemFactory
{
    @Override
    public String getName()
    {
        return "shelf";
    }

    @Override
    public Object create(ConnectorIdentity identity)
    {
        ShelfConfig config = ShelfConfig.fromCatalogProperties(/* TBD: source from catalog props */);
        return ShelfPlugin.INSTANCE.buildFileSystemFactory(
            config,
            /* delegateFactory: native-s3 from registry */,
            new RangeFetcher(config),
            new MembershipResolver(config));
    }
}
```

Estimated diff size: ~25 LOC.

### Patch 2: integration test scaffold

`clients/trino/src/test/java/io/shelf/plugin/TestShelfPluginFileSystemFactory.java`:

```java
@Test
public void shelfPluginExposesFileSystemFactoryViaSpi()
{
    ShelfPlugin plugin = new ShelfPlugin();
    Iterable<TrinoFileSystemFactory> factories = plugin.getFileSystemFactories();
    assertThat(factories).hasSize(1);
    TrinoFileSystemFactory f = factories.iterator().next();
    assertThat(f.getName()).isEqualTo("shelf");
}

@Test
public void shelfFileSystemDelegatesReadsToShelfd() throws IOException
{
    // wire mock shelfd via httpmock; assert reads route through ShelfFileSystem
}
```

Estimated diff size: ~80 LOC.

### Patch 3: chart catalog example update

`charts/shelf/examples/trino-catalog-recipe.yaml` — add option:

```yaml
# OPTION B (post-S5, in-process plugin path):
# Skip the s3.endpoint shim hop entirely; use the in-process ShelfFileSystem.
# Requires: Trino 481+ with Plugin.getFileSystemFactories() (trinodb/trino#NN merged)
# AND shelf-trino-plugin JAR installed in $TRINO_HOME/plugin/shelf/
fs.shelf.enabled=true
fs.shelf.shelfd-endpoint=http://<SHELF_POOL_HOST>:9090
# (s3.endpoint can revert to direct AWS S3; ShelfFileSystem intercepts before that)
```

## Activation procedure (when upstream merges)

1. Operator confirms `trinodb/trino#NN` merged + tagged in a Trino release (likely 481 or 482)
2. Bump `clients/trino/pom.xml` `trino.version` to that release
3. Apply the 3 patches above as a single PR
4. Maven build + run tests + tag shelf-trino-plugin JAR
5. Deploy via Helm: install JAR + flip catalog prop in a single rolling restart
6. Re-run V2 (production-trace replay) to measure JVM-local cache lookup vs HTTP shim hop — expected: shelf wins all warm queries by ~50-100x latency
