using AElf.BaseStorageMapper.TestBase;
using AElf.LinqToElasticSearch;
using Volo.Abp.DependencyInjection;
using Xunit;

namespace AElf.BaseStorageMapper.Tests.Test;

public class ShardingKeyProviderTest : AElfIndexerTestBase<AElfBaseStorageMapperTestsModule> ,ITransientDependency
{
    private readonly IShardingKeyProvider<BlockIndex> _blockIndexShardingKeyProvider;
    private readonly IShardingKeyProvider<BlockIndex> _blockIndexShardingKeyProvider2;
    private readonly IShardingKeyProvider<LogEventIndex> _logEventIndexShardingKeyProvider;

    public ShardingKeyProviderTest()
    {
        _blockIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<BlockIndex>>();
        _blockIndexShardingKeyProvider2 = GetRequiredService<IShardingKeyProvider<BlockIndex>>();

        _logEventIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<LogEventIndex>>();

    }
    
    [Fact]
    public void GetBlockIndexShardingKeyByEntityTest()
    {
        BlockIndex blockIndex = new BlockIndex()
        {
            ChainId = "AELF",
            BlockHeight = 123,
            BlockHash = "0x000000000",
            BlockTime = DateTime.Now,
            Confirmed = true
        };
        List<ShardProviderEntity<BlockIndex>> blockPropertyFuncs = _blockIndexShardingKeyProvider.GetShardingKeyByEntity(blockIndex);
        Assert.True(blockPropertyFuncs != null && blockPropertyFuncs.Count == 2);
        Assert.True(blockPropertyFuncs[0].SharKeyName == "ChainId");
        Assert.True(blockPropertyFuncs[1].SharKeyName == "BlockHeight");
        
        List<ShardProviderEntity<BlockIndex>> blockPropertyFuncs2 = _blockIndexShardingKeyProvider2.GetShardingKeyByEntity(blockIndex);
        Assert.True(blockPropertyFuncs2 != null && blockPropertyFuncs2.Count == 2);
        Assert.True(blockPropertyFuncs2[0].SharKeyName == "ChainId");
        Assert.True(blockPropertyFuncs2[1].SharKeyName == "BlockHeight");
        
        LogEventIndex eventIndex = new LogEventIndex()
        {
            ChainId = "AELF",
            BlockHeight = 123,
            BlockHash = "0x000000000",
            BlockTime = DateTime.Now,
            Confirmed = true
        };
        List<ShardProviderEntity<LogEventIndex>> eventPropertyFuncs = _logEventIndexShardingKeyProvider.GetShardingKeyByEntity(eventIndex);
        Assert.True(eventPropertyFuncs != null && eventPropertyFuncs.Count == 2);
        Assert.True(eventPropertyFuncs[0].SharKeyName == "ChainId");
        Assert.True(eventPropertyFuncs[1].SharKeyName == "BlockHeight");

    }
    
    [Fact]
    public void GetLogEventIndexShardingKeyByEntityTest()
    {
        LogEventIndex eventIndex = new LogEventIndex()
        {
            ChainId = "AELF",
            BlockHeight = 123,
            BlockHash = "0x000000000",
            BlockTime = DateTime.Now,
            Confirmed = true
        };
        List<ShardProviderEntity<LogEventIndex>> propertyFuncs = _logEventIndexShardingKeyProvider.GetShardingKeyByEntity(eventIndex);
        Assert.True(propertyFuncs != null && propertyFuncs.Count == 2);
        Assert.True(propertyFuncs[0].SharKeyName == "ChainId");
        Assert.True(propertyFuncs[1].SharKeyName == "BlockHeight");
    }
}