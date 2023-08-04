using System.Linq.Expressions;
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
    public async Task AddAsync()
    {
        var blockIndex =  new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 10,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);
        
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex.ChainId && p.BlockHeight == blockIndex.BlockHeight;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.First().Id == blockIndex.Id);
        Assert.True(results.First().BlockHeight == blockIndex.BlockHeight);
    }
    
    [Fact]
    public async Task AddOrUpdateAsyncTest()
    {
        var blockIndex =  new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 20,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
        
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex.ChainId && p.BlockHeight == blockIndex.BlockHeight;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.First().Id == blockIndex.Id);
        Assert.True(results.First().BlockHeight == blockIndex.BlockHeight);
        
        var blockIndex2 =  new BlockIndex
        {
            Id = "block002",
            BlockHash = "BlockHash001",
            BlockHeight = 20,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        await _elasticsearchRepository.AddOrUpdateAsync(blockIndex2);
        
        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex.ChainId && p.BlockHeight == blockIndex.BlockHeight && p.Id == blockIndex2.Id;
        results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.First().Id == blockIndex2.Id);
        Assert.True(results.First().BlockHeight == blockIndex2.BlockHeight);
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