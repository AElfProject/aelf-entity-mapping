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
            BlockHeight = 15,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
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
            ChainId = "AELF"
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
            ChainId = "AELF"
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
            ChainId = "AELF"
        };
        var blockIndex2 =  new BlockIndex
        {
            Id = "block002",
            BlockHash = "BlockHash002",
            BlockHeight = 20,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        var blockIndex3 =  new BlockIndex
        {
            Id = "block003",
            BlockHash = "BlockHash003",
            BlockHeight = 30,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
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
            ChainId = "AELF"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);
        blockIndex.LogEventCount = 20;
        await _elasticsearchRepository.UpdateAsync(blockIndex);
        
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        Assert.True(results.First().Id == blockIndex.Id);
        Assert.True(results.First().LogEventCount == blockIndex.LogEventCount);
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
            ChainId = "AELF"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);
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
            ChainId = "AELF"
        };
        blockIndex.BlockHeight = 30;
        await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
        Thread.Sleep(1000);
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex.ChainId && p.Id == blockIndex.Id;
        var results = queryable.Where(expression).ToList();
        Assert.True(!results.IsNullOrEmpty());
        
        await _elasticsearchRepository.DeleteAsync(blockIndex.Id);
        Thread.Sleep(1000);
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
    public async Task Get_Test()
    {
        var blockIndex =  new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 10,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);

        var block = await _elasticsearchRepository.GetAsync(blockIndex.Id);
        block.Id.ShouldBe(blockIndex.Id);
        block.BlockHash.ShouldBe(blockIndex.BlockHash);
        block.BlockTime.ShouldBe(blockIndex.BlockTime);
        block.BlockHeight.ShouldBe(blockIndex.BlockHeight);
        block.LogEventCount.ShouldBe(blockIndex.LogEventCount);
        block.ChainId.ShouldBe(blockIndex.ChainId);
        
        for (int i = 1; i <= 7; i++)
        {
            await _elasticsearchRepository.AddAsync(new BlockIndex
            {
                Id = "block" + i,
                BlockHash = "BlockHash" + i,
                BlockHeight = i,
                BlockTime = DateTime.Now.AddDays(-10 + i),
                LogEventCount = i,
                ChainId = "AELF"
            });
        }
        
        block = await _elasticsearchRepository.GetAsync("block7");
        block.Id.ShouldBe("block7");
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
            ChainId = "AELF"
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
                ChainId = "AELF"
            };
            await _elasticsearchRepository.AddAsync(blockIndex);
        }

        var list = await _elasticsearchRepository.GetListAsync(o => o.ChainId == "AELF" && o.BlockHeight >= 0);
        list.Count.ShouldBe(7);
        var count = await _elasticsearchRepository.GetCountAsync(o => o.ChainId == "AELF" && o.BlockHeight >= 0);
        count.ShouldBe(7);

        list = await _elasticsearchRepository.GetListAsync(o =>
            o.ChainId == "AELF" && o.BlockHeight > 5 && o.LogEventCount > 6);
        list.Count.ShouldBe(1);
        count = await _elasticsearchRepository.GetCountAsync(o =>
            o.ChainId == "AELF" && o.BlockHeight > 5 && o.LogEventCount > 6);
        count.ShouldBe(1);

        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        list = queryable.Where(o => o.ChainId == "AELF" && o.BlockHeight > 0).ToList();
        list.Count.ShouldBe(7);
        count = queryable.Count(o => o.ChainId == "AELF" && o.BlockHeight > 0);
        count.ShouldBe(7);

        list = queryable.Where(o => o.ChainId == "AELF" && o.BlockHeight > 5 && o.LogEventCount > 6).ToList();
        list.Count.ShouldBe(1);
        count = queryable.Count(o => o.ChainId == "AELF" && o.BlockHeight > 5 && o.LogEventCount > 6);
        count.ShouldBe(1);

        list = queryable.Where(o => o.ChainId == "AELF" && o.BlockHeight > 0).OrderBy(o => o.BlockHeight).Take(5)
            .Skip(5).ToList();
        list.Count.ShouldBe(2);
        list[0].BlockHeight.ShouldBe(6);

        list = queryable.Where(o => o.ChainId == "AELF" && o.BlockHash == "BlockHash7").ToList();
        list.Count.ShouldBe(1);
        list[0].Id.ShouldBe("block7");
        count = queryable.Count(o => o.ChainId == "AELF" && o.BlockHash == "BlockHash7");
        count.ShouldBe(1);
    }

    [Fact]
    public async Task GetList_SpecificIndex_Test()
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
                ChainId = "AELF"
            };
            await _elasticsearchRepository.AddAsync(blockIndex, indexName);
        }

        var list = await _elasticsearchRepository.GetListAsync(o => o.ChainId == "AELF" && o.BlockHeight > 0, indexName);
        list.Count.ShouldBe(7);
        var count = await _elasticsearchRepository.GetCountAsync(o => o.ChainId == "AELF" && o.BlockHeight > 0, indexName);
        count.ShouldBe(7);

        list = await _elasticsearchRepository.GetListAsync(o =>
            o.ChainId == "AELF" && o.BlockHeight > 5 && o.LogEventCount > 6, indexName);
        list.Count.ShouldBe(1);
        count = await _elasticsearchRepository.GetCountAsync(o =>
            o.ChainId == "AELF" && o.BlockHeight > 5 && o.LogEventCount > 6, indexName);
        count.ShouldBe(1);

        var queryable = await _elasticsearchRepository.GetQueryableAsync(indexName);
        list = queryable.Where(o => o.ChainId == "AELF" && o.BlockHeight > 0).ToList();
        list.Count.ShouldBe(7);
        count = queryable.Count(o => o.ChainId == "AELF" && o.BlockHeight > 0);
        count.ShouldBe(7);

        list = queryable.Where(o => o.ChainId == "AELF" && o.BlockHeight > 5 && o.LogEventCount > 6).ToList();
        list.Count.ShouldBe(1);
        count = queryable.Count(o => o.ChainId == "AELF" && o.BlockHeight > 5 && o.LogEventCount > 6);
        count.ShouldBe(1);

        list = queryable.Where(o => o.ChainId == "AELF" && o.BlockHeight > 0).OrderBy(o => o.BlockHeight).Take(5)
            .Skip(5).ToList();
        list.Count.ShouldBe(2);
        list[0].BlockHeight.ShouldBe(6);
    }

    [Fact]
    public async Task GetList_MultipleChain_Test()
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
                ChainId = "AELF"
            };
            await _elasticsearchRepository.AddAsync(blockIndex);
        }
        
        for (int i = 1; i <= 11; i++)
        {
            var blockIndex = new BlockIndex
            {
                Id = "block" + i,
                BlockHash = "BlockHash" + i,
                BlockHeight = i,
                BlockTime = DateTime.Now.AddDays(-10 + i),
                LogEventCount = i,
                ChainId = "tDVV"
            };
            await _elasticsearchRepository.AddAsync(blockIndex);
        }
        
        var list = await _elasticsearchRepository.GetListAsync(o =>o.ChainId =="AELF" && o.BlockHeight >= 0);
        list.Count.ShouldBe(7);
        
        list = await _elasticsearchRepository.GetListAsync(o =>o.ChainId =="tDVV" && o.BlockHeight >= 0);
        list.Count.ShouldBe(11);
    }
}