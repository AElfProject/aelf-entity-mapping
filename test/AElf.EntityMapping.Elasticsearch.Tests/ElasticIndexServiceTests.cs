using AElf.EntityMapping.Elasticsearch.Entities;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Sharding;
using Shouldly;
using Volo.Abp.Caching;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch;

public class ElasticIndexServiceTests: AElfElasticsearchTestBase
{
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IDistributedCache<List<CollectionRouteKeyCacheItem>> _indexMarkFieldCache;

    public ElasticIndexServiceTests()
    {
        _elasticIndexService = GetRequiredService<IElasticIndexService>();
        _indexMarkFieldCache= GetRequiredService<IDistributedCache<List<CollectionRouteKeyCacheItem>>>();
    }

    [Fact]
    public async Task InitializeIndexMarkedField_Test()
    {
        await _elasticIndexService.InitializeIndexMarkedFieldAsync(typeof(BlockIndex));
        var cacheName = _elasticIndexService.GetIndexMarkFieldCacheName(typeof(BlockIndex));
        var collectionMarkFieldList = await _indexMarkFieldCache.GetAsync(cacheName);
        collectionMarkFieldList.ShouldNotBeNull();
        collectionMarkFieldList.Count.ShouldBeGreaterThan(1);
    }
}