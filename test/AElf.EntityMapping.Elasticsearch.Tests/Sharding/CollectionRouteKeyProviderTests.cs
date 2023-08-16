using AElf.EntityMapping.Elasticsearch.Entities;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class CollectionRouteKeyProviderTests: AElfElasticsearchTestBase
{
    private readonly ICollectionRouteKeyProvider<BlockIndex> _blockIndexCollectionRouteKeyProvider;
    private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;
    private readonly AElfEntityMappingOptions _option;

    public CollectionRouteKeyProviderTests()
    {
        _blockIndexCollectionRouteKeyProvider = GetRequiredService<ICollectionRouteKeyProvider<BlockIndex>>();
        _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
        _option = GetRequiredService<IOptionsSnapshot<AElfEntityMappingOptions>>().Value;
    }

    [Fact]
    public async Task GetNonShardKeys_Test()
    {
        List<CollectionRouteKeyItem<BlockIndex>> nonShardKeys = await _blockIndexCollectionRouteKeyProvider.GetNonShardKeysAsync();
        
        nonShardKeys.Count.ShouldBe(1);
        nonShardKeys[0].FieldName.ShouldBe(nameof(BlockIndex.BlockHash));
        // nonShardKeys[0].FieldValueType.ShouldBe(typeof(string).ToString());
        // nonShardKeys[0].IsRouteKey.ShouldBeTrue();
    }

    [Fact]
    public async Task GetShardCollectionNameListByConditions_Test()
    {
        await InitBlocksAsync();

        var collectionNameCondition = new List<CollectionNameCondition>();
        var indexes =
            await _blockIndexCollectionRouteKeyProvider.GetCollectionNameAsync(
                collectionNameCondition);
        indexes.Count.ShouldBe(0);
        
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.BlockHash),
            Value = "BlockHash7",
            Type = ConditionType.Equal
        });
        indexes =
            await _blockIndexCollectionRouteKeyProvider.GetCollectionNameAsync(
                collectionNameCondition);
        indexes.Count.ShouldBe(1);
        // indexes[0].ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        indexes[0].ShouldBe($"blockindex-aelf-1");
        
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.BlockHash),
            Value = "BlockHash6",
            Type = ConditionType.Equal
        });
        indexes =
            await _blockIndexCollectionRouteKeyProvider.GetCollectionNameAsync(
                collectionNameCondition);
        indexes.Count.ShouldBe(1);
        // indexes[0].ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        indexes[0].ShouldBe($"blockindex-aelf-1");
        
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.BlockHash),
            Value = "BlockHash1",
            Type = ConditionType.Equal
        });
        indexes =
            await _blockIndexCollectionRouteKeyProvider.GetCollectionNameAsync(
                collectionNameCondition);
        indexes.Count.ShouldBe(0);
    }
    
    [Fact]
    public async Task GetShardCollectionNameById_Test()
    {
        await InitBlocksAsync();
        
        var index = await _blockIndexCollectionRouteKeyProvider.GetCollectionNameAsync("block1");
        // index.ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-0");
        index.ShouldBe($"blockindex-aelf-0");
        
        index = await _blockIndexCollectionRouteKeyProvider.GetCollectionNameAsync("block2");
        // index.ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-0");
        index.ShouldBe($"blockindex-aelf-0");
        
        index = await _blockIndexCollectionRouteKeyProvider.GetCollectionNameAsync("block7");
        // index.ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        index.ShouldBe($"blockindex-aelf-1");
        
        index = await _blockIndexCollectionRouteKeyProvider.GetCollectionNameAsync("block8");
        index.ShouldBeEmpty();
    }

    [Fact]
    public async Task GetNonShardKeyRouteIndex_Test()
    {
        var routeIndex = $"{_option.CollectionPrefix.ToLower()}.blockindex.blockhash.route";
        await InitBlocksAsync();

        var route = await _blockIndexCollectionRouteKeyProvider.GetNonShardKeyRouteIndexAsync("block1", routeIndex);
        route.Id.ShouldBe("block1");
        route.CollectionRouteKey.ShouldBe("BlockHash1");
        // route.ShardCollectionName.ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-0");
        route.CollectionName.ShouldBe($"blockindex-aelf-0");
        
        route = await _blockIndexCollectionRouteKeyProvider.GetNonShardKeyRouteIndexAsync("block6", routeIndex);
        route.Id.ShouldBe("block6");
        route.CollectionRouteKey.ShouldBe("BlockHash6");
        // route.ShardCollectionName.ShouldBe($"{_option.CollectionPrefix.ToLower()}.blockindex-aelf-1");
        route.CollectionName.ShouldBe($"blockindex-aelf-1");
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