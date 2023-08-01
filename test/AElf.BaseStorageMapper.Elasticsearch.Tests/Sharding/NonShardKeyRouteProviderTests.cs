using AElf.BaseStorageMapper.Elasticsearch.Repositories;
using AElf.BaseStorageMapper.Sharding;
using Shouldly;
using Xunit;

namespace AElf.BaseStorageMapper.Elasticsearch.Sharding;

public class NonShardKeyRouteProviderTests: AElfElasticsearchTestBase
{
    private readonly INonShardKeyRouteProvider<BlockIndex> _blockIndexNonShardKeyRouteProvider;

    public NonShardKeyRouteProviderTests()
    {
        _blockIndexNonShardKeyRouteProvider = GetRequiredService<INonShardKeyRouteProvider<BlockIndex>>();
    }

    [Fact]
    public async Task GetNonShardKeys_Test()
    {
        List<CollectionMarkField> nonShardKeys = await _blockIndexNonShardKeyRouteProvider.GetNonShardKeysAsync();
        
        nonShardKeys.Count.ShouldBe(1);
        nonShardKeys[0].FieldName.ShouldBe(nameof(BlockIndex.BlockHash));
        nonShardKeys[0].FieldValueType.ShouldBe(typeof(string).ToString());
        nonShardKeys[0].IsRouteKey.ShouldBeTrue();
    }
}