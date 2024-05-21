using AElf.EntityMapping.Elasticsearch.Entities;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Options;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch;

public class ElasticsearchCollectionNameProviderTests: AElfElasticsearchTestBase
{
    private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;
    private readonly AElfEntityMappingOptions _option;
    private readonly ICollectionNameProvider<BlockIndex> _collectionNameProvider;

    public ElasticsearchCollectionNameProviderTests()
    {
        _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
        _option = GetRequiredService<IOptionsSnapshot<AElfEntityMappingOptions>>().Value;
        _collectionNameProvider = GetRequiredService<ICollectionNameProvider<BlockIndex>>();
    }
    
    [Fact]
    public async Task GetCollectionName_Test()
    {
        await InitBlocksAsync();
        
        var collectionNameCondition = new List<CollectionNameCondition>();
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.ChainId),
            Value = "AELF",
            Type = ConditionType.Equal
        });
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.BlockHeight),
            Value = 7,
            Type = ConditionType.Equal
        });
        var collectionNames = await _collectionNameProvider.GetFullCollectionNameAsync(collectionNameCondition);
        collectionNames.Count.ShouldBe(1);
        collectionNames[0].ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.BlockHash),
            Value = "BlockHash7",
            Type = ConditionType.Equal
        });
        collectionNames = await _collectionNameProvider.GetFullCollectionNameAsync(collectionNameCondition);
        collectionNames.Count.ShouldBe(1);
        collectionNames[0].ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        
        collectionNameCondition.Clear();
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.ChainId),
            Value = "AELF",
            Type = ConditionType.Equal
        });
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.BlockHeight),
            Value = 1,
            Type = ConditionType.Equal
        });
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.BlockHash),
            Value = "BlockHash7",
            Type = ConditionType.Equal
        });
        collectionNames = await _collectionNameProvider.GetFullCollectionNameAsync(collectionNameCondition);
        collectionNames.Count.ShouldBe(1);
    }

    [Fact]
    public async Task GetFullCollectionNameByEntity_Test()
    {
        var collectionNames = await _collectionNameProvider.GetFullCollectionNameByEntityAsync(new BlockIndex
        {
            Id = "block",
            ChainId = "AELF",
            BlockHeight = 1
        });
        collectionNames.Count.ShouldBe(1);
        collectionNames[0].ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-0");
        
        collectionNames = await _collectionNameProvider.GetFullCollectionNameByEntityAsync(new BlockIndex
        {
            Id = "block",
            ChainId = "AELF",
            BlockHeight = 6
        });
        collectionNames.Count.ShouldBe(1);
        collectionNames[0].ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        
        collectionNames = await _collectionNameProvider.GetFullCollectionNameByEntityAsync(new BlockIndex
        {
            Id = "block",
            ChainId = "tDVV",
            BlockHeight = 11
        });
        collectionNames.Count.ShouldBe(1);
        collectionNames[0].ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-tdvv-1");
    }
    
    [Fact]
    public async Task GetFullCollectionNameByEntities_Test()
    {
        var entities = new List<BlockIndex>();
        entities.Add(new BlockIndex
        {
            Id = "block",
            ChainId = "AELF",
            BlockHeight = 1
        });
        var collectionNames = await _collectionNameProvider.GetFullCollectionNameByEntityAsync(entities);
        collectionNames.Count.ShouldBe(1);
        collectionNames[0].ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-0");
        
        entities.Add(new BlockIndex
        {
            Id = "block",
            ChainId = "AELF",
            BlockHeight = 6
        });
        collectionNames = await _collectionNameProvider.GetFullCollectionNameByEntityAsync(entities);
        collectionNames.Count.ShouldBe(2);
        collectionNames.ShouldContain($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-0");
        collectionNames.ShouldContain($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        
        entities.Add(new BlockIndex
        {
            Id = "block",
            ChainId = "tDVV",
            BlockHeight = 11
        });
        collectionNames = await _collectionNameProvider.GetFullCollectionNameByEntityAsync(entities);
        collectionNames.Count.ShouldBe(3);
        collectionNames.ShouldContain($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-0");
        collectionNames.ShouldContain($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        collectionNames.ShouldContain($"{_option.CollectionPrefix.ToLower()}.blockindex-tdvv-1");
    }

    [Fact]
    public async Task GetFullCollectionNameById_Test()
    {
        await InitBlocksAsync();
        
        var collectionNames = await _collectionNameProvider.GetFullCollectionNameByIdAsync("block1");
        collectionNames.ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-0");
        
        collectionNames = await _collectionNameProvider.GetFullCollectionNameByIdAsync("block6");
        collectionNames.ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
    }

    private async Task InitBlocksAsync()
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
    }
}