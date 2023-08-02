using AElf.EntityMapping.Elasticsearch.Services;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch.Repositories;

public class ElasticsearchRepositoryTests : AElfElasticsearchTestBase
{
    private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;
    private readonly IElasticIndexService _elasticIndexService;

    public ElasticsearchRepositoryTests()
    {
        _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
        _elasticIndexService = GetRequiredService<IElasticIndexService>();
    }

    [Fact]
    public async Task QueryTest()
    {
        var block12 = new BlockIndex
        {
            Id = "block12",
            BlockHash = "BlockHash12",
            BlockHeight = 12,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10
        };
        
        var block13 = new BlockIndex
        {
            Id = "block13",
            BlockHash = "BlockHash13",
            BlockHeight = 13,
            BlockTime = DateTime.Now.AddDays(-7),
            LogEventCount = 10
        };
        
        var indexName = "block02";
        
        await _elasticIndexService.CreateIndexAsync(indexName, typeof(BlockIndex), 1, 0);
         await _elasticsearchRepository.AddOrUpdateAsync(block12);
         await _elasticsearchRepository.AddOrUpdateAsync(block13);

        var queryable = await _elasticsearchRepository.GetQueryableAsync(indexName);
        var list = queryable.Where(q =>q.BlockHeight >= 1 && q.BlockHeight < 13).OrderByDescending(o=>o.BlockHeight).Take(2).Skip(1)
             .ToList();
    }
}