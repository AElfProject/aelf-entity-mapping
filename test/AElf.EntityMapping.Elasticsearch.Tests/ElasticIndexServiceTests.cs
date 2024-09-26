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

    public ElasticIndexServiceTests()
    {
        _elasticIndexService = GetRequiredService<IElasticIndexService>();
    }
}