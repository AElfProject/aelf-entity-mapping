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
    private readonly IShardingKeyProvider<TransactionIndex> _logTransationIndexShardingKeyProvider;

    public ShardingKeyProviderTest()
    {
        _blockIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<BlockIndex>>();
        _blockIndexShardingKeyProvider2 = GetRequiredService<IShardingKeyProvider<BlockIndex>>();

        _logEventIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<LogEventIndex>>();
        
        _logTransationIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<TransactionIndex>>();

        
    }
    
    [Fact]
    public void GetShardingKeyByEntityTestV1()
    {
        BlockIndex blockIndex = new BlockIndex()
        {
            ChainId = "AELF",
            BlockHeight = 123,
            BlockHash = "0x000000000",
            BlockTime = DateTime.Now,
            Confirmed = true
        };
        List<ShardProviderEntity<BlockIndex>> blockPropertyFuncs = _blockIndexShardingKeyProvider.GetShardingKeyByEntity(blockIndex.GetType());
        Assert.True(blockPropertyFuncs != null && blockPropertyFuncs.Count == 4);
        Assert.True(blockPropertyFuncs[0].SharKeyName == "ChainId");
        Assert.True(blockPropertyFuncs[1].SharKeyName == "ChainId");
        Assert.True(blockPropertyFuncs[2].SharKeyName == "BlockHeight");
        Assert.True(blockPropertyFuncs[3].SharKeyName == "BlockHeight");
        
        List<ShardProviderEntity<BlockIndex>> blockPropertyFuncs2 = _blockIndexShardingKeyProvider2.GetShardingKeyByEntity(blockIndex.GetType());
        Assert.True(blockPropertyFuncs2 != null && blockPropertyFuncs2.Count == 4);
        Assert.True(blockPropertyFuncs2[0].SharKeyName == "ChainId");
        Assert.True(blockPropertyFuncs2[1].SharKeyName == "ChainId");
        Assert.True(blockPropertyFuncs2[2].SharKeyName == "BlockHeight");
        Assert.True(blockPropertyFuncs2[3].SharKeyName == "BlockHeight");
        
        LogEventIndex eventIndex = new LogEventIndex()
        {
            ChainId = "AELF",
            BlockHeight = 123,
            BlockHash = "0x000000000",
            BlockTime = DateTime.Now,
            Confirmed = true
        };
        List<ShardProviderEntity<LogEventIndex>> eventPropertyFuncs = _logEventIndexShardingKeyProvider.GetShardingKeyByEntity(eventIndex.GetType());
        Assert.True(eventPropertyFuncs != null && eventPropertyFuncs.Count == 4);
        Assert.True(eventPropertyFuncs[0].SharKeyName == "ChainId");
        Assert.True(eventPropertyFuncs[1].SharKeyName == "ChainId");
        Assert.True(eventPropertyFuncs[2].SharKeyName == "BlockHeight");
        Assert.True(eventPropertyFuncs[3].SharKeyName == "BlockHeight");

    }
    
    [Fact]
    public void GetShardingKeyByEntityTestV2()
    {
        LogEventIndex eventIndex = new LogEventIndex()
        {
            ChainId = "AELF",
            BlockHeight = 123,
            BlockHash = "0x000000000",
            BlockTime = DateTime.Now,
            Confirmed = true
        };
        List<ShardProviderEntity<LogEventIndex>> propertyFuncs = _logEventIndexShardingKeyProvider.GetShardingKeyByEntity(eventIndex.GetType());
        Assert.True(propertyFuncs != null && propertyFuncs.Count == 4);
        Assert.True(propertyFuncs[0].SharKeyName == "ChainId");
        Assert.True(propertyFuncs[1].SharKeyName == "ChainId");
        Assert.True(propertyFuncs[2].SharKeyName == "BlockHeight");
        Assert.True(propertyFuncs[3].SharKeyName == "BlockHeight");
    }
    [Fact]
    public void IsShardingCollectionTest()
    {
        BlockIndex blockIndex = new BlockIndex();
        Assert.True(_blockIndexShardingKeyProvider.IsShardingCollection());

        TransactionIndex transactionIndex = new TransactionIndex();
        Assert.False(_logTransationIndexShardingKeyProvider.IsShardingCollection());
        
        LogEventIndex eventIndex = new LogEventIndex();
    }
    
    [Fact]
    public void GetShardingKeyByEntityAndFieldNameTest()
    {
        BlockIndex blockIndex = new BlockIndex()
        {
            ChainId = "AELF",
            BlockHeight = 123,
            BlockHash = "0x000000000",
            BlockTime = DateTime.Now,
            Confirmed = true
        };
        var blockHeight = _blockIndexShardingKeyProvider.GetShardingKeyByEntityAndFieldName(blockIndex, "BlockHeight");
        Assert.True(blockHeight.SharKeyName == "BlockHeight");
        Assert.True(blockHeight.Func(blockIndex).ToString() == "123");
        
        var chainId = _blockIndexShardingKeyProvider.GetShardingKeyByEntityAndFieldName(blockIndex, "ChainId");
        Assert.True(chainId.SharKeyName == "ChainId");
        Assert.True(chainId.Func(blockIndex).ToString() == "AELF");
        
        var blockHash = _blockIndexShardingKeyProvider.GetShardingKeyByEntityAndFieldName(blockIndex, "BlockHash");
        Assert.True(blockHash == null);
    }
    
    [Fact]
    public void GetCollectionNameForReadTest()
    {
        Dictionary<string, object> conditions = new Dictionary<string, object>();
        conditions.Add("ChainId", "AELF");
        conditions.Add("BlockHeight",100000);
        var blockIndexNameMain = _blockIndexShardingKeyProvider.GetCollectionNameForRead(conditions);
        Assert.True(blockIndexNameMain == "AElfIndexer.BlockIndex-AELF-"+100000/2000);
        
        Dictionary<string, object> conditions2 = new Dictionary<string, object>();
        conditions2.Add("ChainId", "tDVV");
        conditions2.Add("BlockHeight",100000);
        var blockIndexNameSide = _blockIndexShardingKeyProvider.GetCollectionNameForRead(conditions);
        Assert.True(blockIndexNameSide == "AElfIndexer.BlockIndex-tDVV-"+100000/1000);
        
    }
    
    [Fact]
    public void GetCollectionNameForWriteTest()
    {
        
    }
}