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
        await _elasticsearchRepository.DeleteAsync(blockIndex);

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
            p.ChainId == blockIndex.ChainId && p.BlockHeight == blockIndex.BlockHeight && p.Id == blockIndex.Id;
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
        
        expression = p =>
            p.ChainId == blockIndex.ChainId && p.BlockHeight == blockIndex.BlockHeight;
        results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.Count >= 2);
        await _elasticsearchRepository.DeleteAsync(blockIndex);
        await _elasticsearchRepository.DeleteAsync(blockIndex2);
    }
    
    [Fact]
    public async Task AddOrUpdateManyAsyncTest()
    {
        var blockIndex1 =  new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 10,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        var blockIndex2 =  new BlockIndex
        {
            Id = "block002",
            BlockHash = "BlockHash002",
            BlockHeight = 20,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        var blockIndex3 =  new BlockIndex
        {
            Id = "block003",
            BlockHash = "BlockHash003",
            BlockHeight = 30,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        var bulkList = new List<BlockIndex> {blockIndex1, blockIndex2, blockIndex3};
        await _elasticsearchRepository.AddOrUpdateManyAsync(bulkList);
        
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex1.ChainId && p.BlockHeight == blockIndex1.BlockHeight && p.Id == blockIndex1.Id;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.First().Id == blockIndex1.Id);
        
        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex2.ChainId && p.BlockHeight == blockIndex2.BlockHeight && p.Id == blockIndex2.Id;
        results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.First().Id == blockIndex2.Id);
        
        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex3.ChainId && p.BlockHeight == blockIndex3.BlockHeight && p.Id == blockIndex3.Id;
        results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.First().Id == blockIndex3.Id);
        await _elasticsearchRepository.DeleteAsync(blockIndex1);
        await _elasticsearchRepository.DeleteAsync(blockIndex2);
        await _elasticsearchRepository.DeleteAsync(blockIndex3);
    }
    
    [Fact]
    public async Task UpdateAsync()
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
        blockIndex.BlockHeight = 30;
        await _elasticsearchRepository.AddAsync(blockIndex);
        await _elasticsearchRepository.UpdateAsync(blockIndex);
        
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.First().Id == blockIndex.Id);
        Assert.True(results.First().BlockHeight == blockIndex.BlockHeight);
        await _elasticsearchRepository.DeleteAsync(blockIndex);
    }
    
    [Fact]
    public async Task DeleteAsyncByEntityTest()
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
        blockIndex.BlockHeight = 30;
        await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        
        await _elasticsearchRepository.DeleteAsync(blockIndex);
        
        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        results = queryable.Where(expression).ToList();
        Assert.True(results.IsNullOrEmpty());
    }

    [Fact]
    public async Task DeleteAsyncByIdTest()
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
        blockIndex.BlockHeight = 30;
        await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        
        await _elasticsearchRepository.DeleteAsync(blockIndex.Id);
        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        results = queryable.Where(expression).ToList();
        Assert.True(results.IsNullOrEmpty());
    }
    
    [Fact]
    public async Task DeleteManyAsyncTest()
    {
        /*var blockIndex1 =  new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 10,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        var blockIndex2 =  new BlockIndex
        {
            Id = "block002",
            BlockHash = "BlockHash002",
            BlockHeight = 20,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        var blockIndex3 =  new BlockIndex
        {
            Id = "block003",
            BlockHash = "BlockHash003",
            BlockHeight = 30,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AElf"
        };
        var bulkList = new List<BlockIndex> {blockIndex1, blockIndex2, blockIndex3};
        await _elasticsearchRepository.AddOrUpdateManyAsync(bulkList);

        await _elasticsearchRepository.DeleteManyAsync(bulkList);
        blockIndex.BlockHeight = 30;
        await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        
        await _elasticsearchRepository.DeleteManyAsync(blockIndex);
        
        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        results = queryable.Where(expression).ToList();
        Assert.True(results.IsNullOrEmpty());*/
    }
    [Fact]
    public async Task QueryTest()
    {
        /*var block12 = new BlockIndex
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
             .ToList();*/
    }
}