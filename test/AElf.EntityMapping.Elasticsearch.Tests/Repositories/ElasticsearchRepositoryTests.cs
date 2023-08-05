using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Options;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch.Repositories;

public class ElasticsearchRepositoryTests : AElfElasticsearchTestBase
{
    private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;
    private readonly IElasticIndexService _elasticIndexService;
    private readonly AElfEntityMappingOptions _option;

    public ElasticsearchRepositoryTests()
    {
        _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
        _elasticIndexService = GetRequiredService<IElasticIndexService>();
        _option = GetRequiredService<IOptionsSnapshot<AElfEntityMappingOptions>>().Value;
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
    public async Task Get_Test()
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

        var block = await _elasticsearchRepository.GetAsync(blockIndex.Id);
        block.Id.ShouldBe(blockIndex.Id);
        block.BlockHash.ShouldBe(blockIndex.BlockHash);
        block.BlockTime.ShouldBe(blockIndex.BlockTime);
        block.BlockHeight.ShouldBe(blockIndex.BlockHeight);
        block.LogEventCount.ShouldBe(blockIndex.LogEventCount);
        block.ChainId.ShouldBe(blockIndex.ChainId);
    }

    [Fact]
    public async Task Get_SpecificIndex_Test()
    {
        var indexName = $"{_option.CollectionPrefix}.block".ToLower();
        var blockIndex =  new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 10,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        await _elasticsearchRepository.AddAsync(blockIndex, indexName);

        var block = await _elasticsearchRepository.GetAsync(blockIndex.Id, indexName);
        block.Id.ShouldBe(blockIndex.Id);
        block.BlockHash.ShouldBe(blockIndex.BlockHash);
        block.BlockTime.ShouldBe(blockIndex.BlockTime);
        block.BlockHeight.ShouldBe(blockIndex.BlockHeight);
        block.LogEventCount.ShouldBe(blockIndex.LogEventCount);
        block.ChainId.ShouldBe(blockIndex.ChainId);
    }

    [Fact]
    public async Task GetList_Test()
    {
        for (int i = 1; i <= 7; i++)
        {
            var blockIndex = new BlockIndex
            {
                Id = "block" + i,
                BlockHash = "BlockHash" + i,
                BlockHeight = i,
                BlockTime = DateTime.Now.AddDays(-10 + i),
                LogEventCount = i,
                ChainId = "AElf"
            };
            await _elasticsearchRepository.AddAsync(blockIndex);
        }

        var list = await _elasticsearchRepository.GetListAsync(o => o.ChainId == "AElf" && o.BlockHeight > 0);
        list.Count.ShouldBe(7);

        list = await _elasticsearchRepository.GetListAsync(o =>
            o.ChainId == "AElf" && o.BlockHeight > 5 && o.LogEventCount > 6);
        list.Count.ShouldBe(1);

        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        list = queryable.Where(o => o.ChainId == "AElf" && o.BlockHeight > 0).ToList();
        list.Count.ShouldBe(7);

        list = queryable.Where(o => o.ChainId == "AElf" && o.BlockHeight > 5 && o.LogEventCount > 6).ToList();
        list.Count.ShouldBe(1);

        list = queryable.Where(o => o.ChainId == "AElf" && o.BlockHeight > 0).OrderBy(o => o.BlockHeight).Take(5)
            .Skip(5).ToList();
        list.Count.ShouldBe(2);
        
        list = queryable.Where(o => o.ChainId == "AElf" && o.BlockHash == "BlockHash7").ToList();
        list.Count.ShouldBe(1);
    }

    [Fact]
    public async Task GetList_SpecificIndexTest()
    {
        var indexName = $"{_option.CollectionPrefix}.block".ToLower();
        await _elasticIndexService.CreateIndexAsync(indexName, typeof(BlockIndex), 1, 0);
        
        for (int i = 1; i <= 7; i++)
        {
            var blockIndex = new BlockIndex
            {
                Id = "block" + i,
                BlockHash = "BlockHash" + i,
                BlockHeight = i,
                BlockTime = DateTime.Now.AddDays(-10 + i),
                LogEventCount = i,
                ChainId = "AElf"
            };
            await _elasticsearchRepository.AddAsync(blockIndex, indexName);
        }

        var list = await _elasticsearchRepository.GetListAsync(o => o.ChainId == "AElf" && o.BlockHeight > 0, indexName);
        list.Count.ShouldBe(7);

        list = await _elasticsearchRepository.GetListAsync(o =>
            o.ChainId == "AElf" && o.BlockHeight > 5 && o.LogEventCount > 6, indexName);
        list.Count.ShouldBe(1);

        var queryable = await _elasticsearchRepository.GetQueryableAsync(indexName);
        list = queryable.Where(o => o.ChainId == "AElf" && o.BlockHeight > 0).ToList();
        list.Count.ShouldBe(7);

        list = queryable.Where(o => o.ChainId == "AElf" && o.BlockHeight > 5 && o.LogEventCount > 6).ToList();
        list.Count.ShouldBe(1);

        list = queryable.Where(o => o.ChainId == "AElf" && o.BlockHeight > 0).OrderBy(o => o.BlockHeight).Take(5)
            .Skip(5).ToList();
        list.Count.ShouldBe(2);
    }
}