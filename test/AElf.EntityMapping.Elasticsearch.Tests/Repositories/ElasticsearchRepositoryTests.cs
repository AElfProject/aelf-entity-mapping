using System.Linq.Dynamic.Core;
using System.Linq.Expressions;
using System.Transactions;
using AElf.EntityMapping.Elasticsearch.Entities;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Options;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch.Repositories;

public class ElasticsearchRepositoryTests : AElfElasticsearchTestBase
{
    private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;
    private readonly IElasticsearchRepository<TransactionIndex, string> _transactionIndexRepository;
    private readonly IElasticsearchRepository<AccountBalanceEntity, string> _accountBalanceRepository;
    private readonly IElasticIndexService _elasticIndexService;
    private readonly AElfEntityMappingOptions _option;

    public ElasticsearchRepositoryTests()
    {
        _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
        _transactionIndexRepository = GetRequiredService<IElasticsearchRepository<TransactionIndex, string>>();
        _accountBalanceRepository = GetRequiredService<IElasticsearchRepository<AccountBalanceEntity, string>>();
        _elasticIndexService = GetRequiredService<IElasticIndexService>();
        _option = GetRequiredService<IOptionsSnapshot<AElfEntityMappingOptions>>().Value;
    }

    [Fact]
    public async Task AddAsync()
    {
        var blockIndex = new BlockIndex
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
        var blockIndex = new BlockIndex
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

        var blockIndex2 = new BlockIndex
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
        var blockIndex1 = new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 10,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        var blockIndex2 = new BlockIndex
        {
            Id = "block002",
            BlockHash = "BlockHash002",
            BlockHeight = 20,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        var blockIndex3 = new BlockIndex
        {
            Id = "block003",
            BlockHash = "BlockHash003",
            BlockHeight = 30,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        var bulkList = new List<BlockIndex> { blockIndex1, blockIndex2, blockIndex3 };
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
    public async Task UpdateAsyncTest()
    {
        var blockIndex = new BlockIndex
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
    public async Task UpdateManyAsyncTest()
    {
        var blockIndex1 = new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 10,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        var blockIndex2 = new BlockIndex
        {
            Id = "block002",
            BlockHash = "BlockHash002",
            BlockHeight = 20,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        var blockIndex3 = new BlockIndex
        {
            Id = "block003",
            BlockHash = "BlockHash003",
            BlockHeight = 30,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        var bulkList = new List<BlockIndex> { blockIndex1, blockIndex2, blockIndex3 };
        await _elasticsearchRepository.AddOrUpdateManyAsync(bulkList);

        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex1.ChainId && p.BlockHeight == blockIndex1.BlockHeight && p.Id == blockIndex1.Id;
        var results = queryable.Where(expression).ToList();
        results.ShouldNotBeNull();
        results.First().BlockHash.ShouldBe(blockIndex1.BlockHash);
        results.First().LogEventCount.ShouldBe(blockIndex1.LogEventCount);

        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex2.ChainId && p.BlockHeight == blockIndex2.BlockHeight && p.Id == blockIndex2.Id;
        results = queryable.Where(expression).ToList();
        results.ShouldNotBeNull();
        results.First().BlockHash.ShouldBe(blockIndex2.BlockHash);
        results.First().LogEventCount.ShouldBe(blockIndex2.LogEventCount);

        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex3.ChainId && p.BlockHeight == blockIndex3.BlockHeight && p.Id == blockIndex3.Id;
        results = queryable.Where(expression).ToList();
        results.ShouldNotBeNull();
        results.First().BlockHash.ShouldBe(blockIndex3.BlockHash);
        results.First().LogEventCount.ShouldBe(blockIndex3.LogEventCount);

        blockIndex1.LogEventCount = 100;
        blockIndex2.LogEventCount = 200;
        blockIndex3.LogEventCount = 300;
        var bulkUpdateList = new List<BlockIndex> { blockIndex1, blockIndex2, blockIndex3 };
        await _elasticsearchRepository.UpdateManyAsync(bulkUpdateList);

        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex1.ChainId && p.BlockHeight == blockIndex1.BlockHeight && p.Id == blockIndex1.Id;
        results = queryable.Where(expression).ToList();
        results.ShouldNotBeNull();
        results.First().LogEventCount.ShouldBe(100);

        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex2.ChainId && p.BlockHeight == blockIndex2.BlockHeight && p.Id == blockIndex2.Id;
        results = queryable.Where(expression).ToList();
        results.ShouldNotBeNull();
        results.First().LogEventCount.ShouldBe(200);

        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex3.ChainId && p.BlockHeight == blockIndex3.BlockHeight && p.Id == blockIndex3.Id;
        results = queryable.Where(expression).ToList();
        results.ShouldNotBeNull();
        results.First().LogEventCount.ShouldBe(300);
    }

    [Fact]
    public async Task DeleteAsyncByEntityTest()
    {
        var blockIndex = new BlockIndex
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
        var blockIndex = new BlockIndex
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
        var blockIndex1 = new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash001",
            BlockHeight = 1,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 10,
            ChainId = "AELF"
        };
        var blockIndex2 = new BlockIndex
        {
            Id = "block002",
            BlockHash = "BlockHash002",
            BlockHeight = 2,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 20,
            ChainId = "AELF"
        };
        var blockIndex3 = new BlockIndex
        {
            Id = "block003",
            BlockHash = "BlockHash003",
            BlockHeight = 3,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 30,
            ChainId = "AELF"
        };
        var blockIndex30 = new BlockIndex
        {
            Id = "block030",
            BlockHash = "BlockHash030",
            BlockHeight = 30,
            BlockTime = DateTime.Now.AddDays(-8),
            LogEventCount = 30,
            ChainId = "AELF"
        };
        var bulkList = new List<BlockIndex> { blockIndex1, blockIndex2, blockIndex3, blockIndex30 };
        await _elasticsearchRepository.AddOrUpdateManyAsync(bulkList);

        await _elasticsearchRepository.DeleteManyAsync(bulkList);
        var queryable = await _elasticsearchRepository.GetQueryableAsync();

        Expression<Func<BlockIndex, bool>> expression = p =>
            p.ChainId == blockIndex1.ChainId && p.Id == blockIndex1.Id;
        Thread.Sleep(500);
        var results = queryable.Where(expression).ToList();
        Assert.True(results.IsNullOrEmpty());

        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex2.ChainId && p.Id == blockIndex2.Id;
        results = queryable.Where(expression).ToList();
        Assert.True(results.IsNullOrEmpty());

        queryable = await _elasticsearchRepository.GetQueryableAsync();
        expression = p =>
            p.ChainId == blockIndex30.ChainId && p.Id == blockIndex30.Id;
        results = queryable.Where(expression).ToList();
        Assert.True(results.IsNullOrEmpty());
    }

    [Fact]
    public async Task Get_Test()
    {
        var blockIndex = new BlockIndex
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

        // for (int i = 1; i <= 7; i++)
        // {
        //     await _elasticsearchRepository.AddAsync(new BlockIndex
        //     {
        //         Id = "block" + i,
        //         BlockHash = "BlockHash" + i,
        //         BlockHeight = i,
        //         BlockTime = DateTime.Now.AddDays(-10 + i),
        //         LogEventCount = i,
        //         ChainId = "AELF"
        //     });
        // }
        //
        // block = await _elasticsearchRepository.GetAsync("block7");
        // block.Id.ShouldBe("block7");
    }

    [Fact]
    public async Task Get_SpecificIndex_Test()
    {
        var indexName = $"{_option.CollectionPrefix}.block".ToLower();
        var blockIndex = new BlockIndex
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

    private async Task ClearTransactionIndex(string chainId, long startBlockNumber, long endBlockNumber)
    {
        Expression<Func<TransactionIndex, bool>> expression = p => p.ChainId == chainId && p.BlockHeight >= startBlockNumber && p.BlockHeight <= endBlockNumber;
        var queryable = await _transactionIndexRepository.GetQueryableAsync();
        var filterList = queryable.Where(expression).ToList();
        foreach (var deleteTransaction in filterList)
        {
            await _transactionIndexRepository.DeleteAsync(deleteTransaction);
        }
    }

    [Fact]
    public async Task GetList_Nested_Test()
    {
        //clear data for unit test
        ClearTransactionIndex("AELF", 100, 110);

        Thread.Sleep(2000);
        //Unit Test 14
        var transaction_100_1 = MockNewTransactionEtoData(100, false, "token_contract_address", "DonateResourceToken");
        var transaction_100_2 = MockNewTransactionEtoData(100, false, "", "");
        var transaction_100_3 = MockNewTransactionEtoData(100, false, "consensus_contract_address", "UpdateValue");
        var transaction_110 = MockNewTransactionEtoData(110, true, "consensus_contract_address", "UpdateTinyBlockInformation");
        await _transactionIndexRepository.AddAsync(transaction_100_1);
        await _transactionIndexRepository.AddAsync(transaction_100_2);
        await _transactionIndexRepository.AddAsync(transaction_100_3);
        await _transactionIndexRepository.AddAsync(transaction_110);
        Thread.Sleep(2000);

        var chainId = "AELF";
        Expression<Func<TransactionIndex, bool>> mustQuery = p => p.LogEvents.Any(i => i.ChainId == chainId && i.BlockHeight >= 100 && i.BlockHeight <= 110);
        mustQuery = p => p.LogEvents.Any(i => i.ChainId == "AELF" && i.TransactionId == transaction_100_1.TransactionId);
        var queryable = await _transactionIndexRepository.GetQueryableAsync();
        var filterList = queryable.Where(mustQuery).ToList();
        filterList.Count.ShouldBe(1);
    }

    [Fact]
    public async Task GetList_Terms_Test()
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

        List<string> inputs = new List<string>()
        {
            "BlockHash2",
            "BlockHash3",
            "BlockHash4"
        };
        
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        
        var predicates = inputs
            .Select(s => (Expression<Func<BlockIndex, bool>>)(info => info.BlockHash == s))
            .Aggregate((prev, next) => prev.Or(next));
        var filterList_predicate = queryable.Where(predicates).ToList();
        filterList_predicate.Count.ShouldBe(3);
        
        var filterList = queryable.Where(item => inputs.Contains(item.BlockHash)).ToList();
        filterList.Count.ShouldBe(3);

        List<long> heights = new List<long>()
        {
            4, 5
        };
        Expression<Func<BlockIndex, bool>> mustQuery = item => heights.Contains(item.BlockHeight);
        var filterList_heights = queryable.Where(mustQuery).ToList();
        filterList_heights.Count.ShouldBe(2);
    }

    [Fact]
    public async Task GetNestedList_Terms_Test()
    {
        //clear data for unit test
        ClearTransactionIndex("AELF", 100, 110);

        Thread.Sleep(2000);
        //Unit Test 14
        var transaction_100 = MockNewTransactionEtoData(100, false, "token_contract_address", "DonateResourceToken");
        var transaction_101 = MockNewTransactionEtoData(101, false, "", "");
        var transaction_103 = MockNewTransactionEtoData(103, false, "consensus_contract_address", "UpdateValue");
        var transaction_110 = MockNewTransactionEtoData(110, true, "consensus_contract_address", "UpdateTinyBlockInformation");
        await _transactionIndexRepository.AddAsync(transaction_100);
        await _transactionIndexRepository.AddAsync(transaction_101);
        await _transactionIndexRepository.AddAsync(transaction_103);
        await _transactionIndexRepository.AddAsync(transaction_110);
        
        List<long> inputs = new List<long>()
        {
            101,
            103
        };
        var queryable_predicate = await _transactionIndexRepository.GetQueryableAsync();
        var predicates = inputs
            .Select(s => (Expression<Func<TransactionIndex, bool>>)(info => info.LogEvents.Any(x => x.BlockHeight == s)))
            .Aggregate((prev, next) => prev.Or(next));
        var filterList_predicate = queryable_predicate.Where(predicates).ToList();
        filterList_predicate.Count.ShouldBe(2);

        Expression<Func<TransactionIndex, bool>> mustQuery = item =>
            item.LogEvents.Any(x => inputs.Contains(x.BlockHeight));
        
        var queryable = await _transactionIndexRepository.GetQueryableAsync();
        var filterList = queryable.Where(mustQuery).ToList();
        filterList.Count.ShouldBe(2);
    }

    [Fact]
    public async Task SubObjectQueryTest()
    {
        for (int i = 1; i <= 7; i++)
        {
            var accountBalanceEntity = new AccountBalanceEntity
            {
                Id = "block" + i,
                Account = "BlockHash" + i,
                Amount = i,
                Symbol = "AELF",
                Metadata = new Metadata()
                {
                    ChainId = "tDVV",
                    Block=new BlockMetadata()
                    {
                        BlockHash = "BlockHash" + i,
                        BlockHeight = i,
                        BlockTime = DateTime.Now.AddDays(-10 + i)
                    },
                    IsDeleted=false
                }
            };
            await _accountBalanceRepository.AddAsync(accountBalanceEntity);
        }
        
        var queryable = await _accountBalanceRepository.GetQueryableAsync();
        var list1 = queryable.Where(o => o.Metadata.Block.BlockHash == "BlockHash5").ToList();
        // var list1 = queryable.Where(o => o.Metadata.ChainId == "tDVV").ToList();
        // var list1 = queryable.Where(o => o.Account == "BlockHash3").ToList();
        list1.Count.ShouldBe(1);
        
        var list = await _accountBalanceRepository.GetListAsync(o=>o.Metadata.Block.BlockHash == "BlockHash4");
        // var list = await _accountBalanceRepository.GetListAsync(o => o.Account == "BlockHash3");
        list.Count.ShouldBe(1);
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
                ChainId = "AELF",
                TxnFee = "BlockHash" + i,
                Fee = new FeeIndex()
                {
                    BlockFee = "BlockHash" + i,
                    Fee = i % 4
                }
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

        var chainId = "AELF";
        var list = await _elasticsearchRepository.GetListAsync(o => o.ChainId == chainId && o.BlockHeight >= 0 && o.Fee.BlockFee == "BlockHash2");
        list.Count.ShouldBe(1);
        foreach (var e in list)
        {
            e.ChainId.ShouldBe(chainId);
            e.BlockHeight.ShouldBeGreaterThan(0);
            e.Fee.BlockFee.ShouldBe("BlockHash2");
        }

        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        var list1 = queryable.Where(o => o.ChainId == chainId).OrderByDescending(o => o.LogEventCount).ToList();
        list1.Count.ShouldBe(7);
        list1.First().LogEventCount.ShouldBe(7);
        foreach (var e in list1)
        {
            e.ChainId.ShouldBe(chainId);
        }

        var list2 = queryable.Where(o => o.ChainId == chainId && o.BlockHeight >= 0 && o.Fee.Fee == 3).ToList();
        list2.Count.ShouldBe(2);
        foreach (var e in list2)
        {
            e.ChainId.ShouldBe(chainId);
            e.BlockHeight.ShouldBeGreaterThanOrEqualTo(0);
            e.Fee.Fee.ShouldBe(3);
        }

        var list3 = queryable.Where(o => o.ChainId == chainId && o.BlockHeight >= 2).OrderBy(o => o.Fee.Fee).ToList();
        list3.Count.ShouldBe(6);
        list3.First().Fee.Fee.ShouldBe(0);
        foreach (var e in list3)
        {
            e.ChainId.ShouldBe(chainId);
            e.BlockHeight.ShouldBeGreaterThanOrEqualTo(2);
        }
    }

    [Fact]
    public async Task GetList_WildCard_Test()
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
                ChainId = "AELF",
                TxnFee = "BlockHash" + i,
                Fee = new FeeIndex()
                {
                    BlockFee = "BlockHash" + i,
                    Fee = i % 4
                }
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

        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        var list4 = queryable.Where(o => o.ChainId == "AELF" && o.BlockHash.StartsWith("BlockHash")).ToList();
        list4.Count.ShouldBe(7);

        var list5 = queryable.Where(o => o.ChainId == "AELF" && o.Id.Contains("6")).ToList();
        list5.Count.ShouldBe(1);
        list5.First().Id.ShouldBe("block6");

        var list6 = queryable.Where(o => o.ChainId == "AELF" && o.BlockHash.EndsWith("7")).ToList();
        list6.Count.ShouldBe(1);
        list6.First().Id.ShouldBe("block7");

        var list7 = queryable.Where(o => o.ChainId == "AELF" && o.Fee.BlockFee.StartsWith("BlockHash")).ToList();
        list7.Count.ShouldBe(7);

        var list8 = queryable.Where(o => o.ChainId == "AELF" && o.Fee.BlockFee.Contains("6")).ToList();
        list8.Count.ShouldBe(1);
        list8.First().Id.ShouldBe("block6");

        var list9 = queryable.Where(o => o.ChainId == "AELF" && o.Fee.BlockFee.EndsWith("7")).ToList();
        list9.Count.ShouldBe(1);
        list9.First().Id.ShouldBe("block7");
    }


    public static TransactionIndex MockNewTransactionEtoData(long blockHeight, bool isConfirmed, string contractAddress, string eventName)
    {
        string currentBlockHash = CreateBlockHash();
        string transactionId = CreateBlockHash();
        var newTransaction = new TransactionIndex()
        {
            Id = transactionId,
            TransactionId = transactionId,
            ChainId = "AELF",
            From = "2pL7foxBhMC1RVZMUEtkvYK4pWWaiLHBAQcXFdzfD5oZjYSr3e",
            To = "pGa4e5hNGsgkfjEGm72TEvbF7aRDqKBd4LuXtab4ucMbXLcgJ",
            BlockHash = currentBlockHash,
            BlockHeight = blockHeight,
            BlockTime = DateTime.Now,
            MethodName = "UpdateValue",
            Params =
                "CiIKINnB4HhmTpMScNl9T4hNoR1w8dOpx0O684p+pwfm6uOkEiIKINZUk1v+szUqMZZ3w0phLn7qqOX+h+uD1fdP59LYm9CsIiIKIPGQ8ga2zO3CXXtZkjlMma6CI7VSQFA7ZMYrIU6zGT/uKgYInMjcmAYwAVDPAlqpAQqCATA0YmNkMWM4ODdjZDBlZGJkNGNjZjhkOWQyYjNmNzJlNzI1MTFhYTYxODMxOTk2MDAzMTM2ODdiYTZjNTgzZjEzYzNkNmQ3MTZmYTQwZGY4NjA0YWFlZDBmY2FiMzExMzVmZTNjMmQ0NWMwMDk4MDBjMDc1MjU0YTM3ODJiNGM0ZGISIgog8ZDyBrbM7cJde1mSOUyZroIjtVJAUDtkxishTrMZP+5g0AI=",
            Signature =
                "1KblGpvuuo+HSDdh0OhRq/vg3Ts4HoqcIwBeni/356pdEbgnnR2yqbpgvzNs+oNeBb4Ux2kE1XY9lk+p60LfWgA=",
            Index = 0,
            Status = TransactionStatus.Committed,
            Confirmed = isConfirmed,
            ExtraProperties = new Dictionary<string, string>()
            {
                ["Version"] = "0",
                ["RefBlockNumber"] = "335",
                ["RefBlockPrefix"] = "156ff372",
                ["Bloom"] =
                    "AAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAEAAAAAAAAgAAABAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAgAAAAAAAAAABAAAAAAAAAAAAAAIQAAAAABAAAAAgAAAAAAAAAAAAAAAAABACAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAIgAAAAAAAAAAAAAAAAAAAAAAAEAABAAAAAAAAAAAAAAAAAAAAgAAAAgAAAAABAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAgAAAAAAAAAAAAAA==",
                ["ReturnValue"] = "",
                ["Error"] = ""
            },
            LogEvents = new List<LogEvent>()
            {
                new LogEvent()
                {
                    ChainId = "AELF",
                    BlockHash = currentBlockHash,
                    BlockHeight = blockHeight,
                    BlockTime = DateTime.Now,
                    Confirmed = isConfirmed,
                    TransactionId = transactionId,
                    ContractAddress = contractAddress,
                    EventName = eventName,
                    Index = 0,
                    ExtraProperties = new Dictionary<string, string>()
                    {
                        ["Indexed"] =
                            "[ \"CoIBMDRiY2QxYzg4N2NkMGVkYmQ0Y2NmOGQ5ZDJiM2Y3MmU3MjUxMWFhNjE4MzE5OTYwMDMxMzY4N2JhNmM1ODNmMTNjM2Q2ZDcxNmZhNDBkZjg2MDRhYWVkMGZjYWIzMTEzNWZlM2MyZDQ1YzAwOTgwMGMwNzUyNTRhMzc4MmI0YzRkYg==\", \"EgsIxNqlmQYQ4M74LQ==\", \"GhpVcGRhdGVUaW55QmxvY2tJbmZvcm1hdGlvbg==\", \"ILAi\", \"KiIKID3kBhYftHeFZBYS6VOXPeigGAAwZWM85SlzN48xJARW\" ]",
                        ["NonIndexed"] = ""
                    }
                }
            }
        };
        return newTransaction;
    }

    public static string CreateBlockHash()
    {
        return Guid.NewGuid().ToString("N") + Guid.NewGuid().ToString("N");
    }
    
    [Fact]
    public async Task After_Test()
    {
        var blockIndex = new BlockIndex
        {
            Id = "block010",
            BlockHash = "BlockHash",
            BlockHeight = 10,
            BlockTime = DateTime.Now,
            ChainId = "AELF"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);
        
        blockIndex = new BlockIndex
        {
            Id = "block005",
            BlockHash = "BlockHash",
            BlockHeight = 10,
            BlockTime = DateTime.Now,
            ChainId = "AELF"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);
        
        blockIndex = new BlockIndex
        {
            Id = "block009",
            BlockHash = "BlockHash",
            BlockHeight = 9,
            BlockTime = DateTime.Now,
            ChainId = "AELF"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);
        
        blockIndex = new BlockIndex
        {
            Id = "block008",
            BlockHash = "BlockHash",
            BlockHeight = 12,
            BlockTime = DateTime.Now,
            ChainId = "AELF"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);
        
        blockIndex = new BlockIndex
        {
            Id = "block001",
            BlockHash = "BlockHash",
            BlockHeight = 1,
            BlockTime = DateTime.Now,
            ChainId = "AELF"
        };
        await _elasticsearchRepository.AddAsync(blockIndex);
        
        
        var queryable = await _elasticsearchRepository.GetQueryableAsync();
        queryable = queryable.Where(o => o.ChainId == "AELF" && o.BlockHeight >= 1).OrderBy(o=>o.BlockHeight).OrderBy(o=>o.Id).After(new object[]{9,"block009"});
        var list = queryable.ToList();
        list.Count.ShouldBe(3);
        list[0].Id.ShouldBe("block005");
        list[1].Id.ShouldBe("block010");
        list[2].Id.ShouldBe("block008");
    }
}