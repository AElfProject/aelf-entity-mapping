using AElf.BaseStorageMapper.Elasticsearch.Services;
using Xunit;

namespace AElf.BaseStorageMapper.Elasticsearch.Repositories;

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
    public async Task Test()
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

        var indexName = "block, block02";
        
        // await _elasticIndexService.CreateIndexAsync("block"+"02", typeof(BlockIndex), 1, 0);
        //
        // await _elasticsearchRepository.AddOrUpdateAsync(block12, "block"+"02");
        // await _elasticsearchRepository.AddOrUpdateAsync(block13, "block"+"02");

        var queryable = await _elasticsearchRepository.GetQueryableAsync(indexName);
        var list = queryable.Where(q =>q.BlockHeight >= 1 && q.BlockHeight < 13).OrderByDescending(o=>o.BlockHeight).Take(2).Skip(1)
            .ToList();
        ;

    }
}