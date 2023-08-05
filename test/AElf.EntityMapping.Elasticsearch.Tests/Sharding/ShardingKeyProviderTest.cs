using AElf.EntityMapping.Elasticsearch.Entities;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Sharding;
using AElf.EntityMapping.TestBase;
using Volo.Abp.DependencyInjection;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch.Sharding
{
    public class ShardingKeyProviderTest : AElfEntityMappingTestBase<AElfElasticsearchTestsModule> ,ITransientDependency
    {
        private readonly IShardingKeyProvider<BlockIndex> _blockIndexShardingKeyProvider;
        private readonly IShardingKeyProvider<BlockIndex> _blockIndexShardingKeyProvider2;
        private readonly IShardingKeyProvider<LogEventIndex> _logEventIndexShardingKeyProvider;
        private readonly IShardingKeyProvider<TransactionIndex> _logTransationIndexShardingKeyProvider;
        private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;


        public ShardingKeyProviderTest()
        {
            _blockIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<BlockIndex>>();
            _blockIndexShardingKeyProvider2 = GetRequiredService<IShardingKeyProvider<BlockIndex>>();

            _logEventIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<LogEventIndex>>();
        
            _logTransationIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<TransactionIndex>>();

            _logTransationIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<TransactionIndex>>();
            _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
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
            Assert.True(_blockIndexShardingKeyProvider.IsShardingCollection());

            TransactionIndex transactionIndex = new TransactionIndex();
            Assert.False(_logTransationIndexShardingKeyProvider.IsShardingCollection());
            Assert.False(_logTransationIndexShardingKeyProvider.IsShardingCollection());
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
        public void GetCollectionNameTest()
        {
            List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
            CollectionNameCondition condition1 = new CollectionNameCondition();
            condition1.Key = "ChainId";
            condition1.Value = "AELF";
            condition1.Type = ConditionType.Equal;
            CollectionNameCondition condition2 = new CollectionNameCondition();
            condition2.Key = "BlockHeight";
            condition2.Value = "10";
            condition1.Type = ConditionType.Equal;
            conditions.Add(condition1);
            conditions.Add(condition2);
            var blockIndexNameMain = _blockIndexShardingKeyProvider.GetCollectionName(conditions);
            Assert.True(blockIndexNameMain.Count == 1);
            Assert.True(blockIndexNameMain.First().StartsWith("aelfentitymappingtest.blockindex-aelf-"));
        }


        [Fact]
        public void GetCollectionNameForReadTest()
        {
            Dictionary<string, object> conditions = new Dictionary<string, object>();
            conditions.Add("ChainId", "AELF");
            conditions.Add("BlockHeight",100000);
            var blockIndexNameMain = _blockIndexShardingKeyProvider.GetCollectionName(conditions);
            Assert.True(blockIndexNameMain.StartsWith("aelfentitymappingtest.blockindex-aelf-"));
        
            Dictionary<string, object> conditions2 = new Dictionary<string, object>();
            conditions2.Add("ChainId", "tDVV");
            conditions2.Add("BlockHeight",100000);
            var blockIndexNameSide = _blockIndexShardingKeyProvider.GetCollectionName(conditions2);
            Assert.True(blockIndexNameSide.StartsWith("aelfentitymappingtest.blockindex-tdvv-"));
        }
    
        [Fact]
        public void GetCollectionNameForWriteTest()
        {
            BlockIndex blockIndex = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 5,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            var blockIndexNameMain = _blockIndexShardingKeyProvider.GetCollectionName(blockIndex);
            Assert.True(blockIndexNameMain.StartsWith("aelfentitymappingtest.blockindex-aelf-"));
            blockIndex.BlockHeight = 10;
            _blockIndexShardingKeyProvider.GetCollectionName(blockIndex);

            BlockIndex blockIndex2 = new BlockIndex()
            {
                ChainId = "tDVV",
                BlockHeight = 5,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            var blockIndexNameSide = _blockIndexShardingKeyProvider.GetCollectionName(blockIndex2);
            Assert.True(blockIndexNameSide.StartsWith("aelfentitymappingtest.blockindex-tdvv-"));
        }

        private void WriteBlockIndex()
        {
            var blockIndex =  new BlockIndex
            {
                Id = "block001",
                BlockHash = "BlockHash001",
                BlockHeight = 1,
                BlockTime = DateTime.Now.AddDays(-8),
                LogEventCount = 10,
                ChainId = "AELF"
            };
            _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
            Thread.Sleep(500);
            blockIndex.BlockHeight = 5;
            blockIndex.BlockHash = "BlockHash005";
            blockIndex.Id = "block005";
            _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
            Thread.Sleep(500);
            blockIndex.BlockHeight = 10;
            blockIndex.BlockHash = "BlockHash010";
            blockIndex.Id = "block010";
            _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
            Thread.Sleep(500);
        }
    
        private void DelBlockIndex()
        {
            var blockIndex =  new BlockIndex
            {
                Id = "block001",
                BlockHash = "BlockHash001",
                BlockHeight = 1,
                BlockTime = DateTime.Now.AddDays(-8),
                LogEventCount = 10,
                ChainId = "AELF"
            };
            _elasticsearchRepository.DeleteAsync(blockIndex);
            blockIndex.BlockHeight = 5;
            blockIndex.BlockHash = "BlockHash005";
            blockIndex.Id = "block005";
            _elasticsearchRepository.DeleteAsync(blockIndex);
            blockIndex.BlockHeight = 10;
            blockIndex.BlockHash = "BlockHash010";
            blockIndex.Id = "block010";
            _elasticsearchRepository.DeleteAsync(blockIndex);
        }


        [Fact]
        public void GetCollectionNameEqual()
        {
            GetCollectionNameForWriteTest();
            List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
            CollectionNameCondition condition1 = new CollectionNameCondition();
            condition1.Key = "ChainId";
            condition1.Value = "AELF";
            condition1.Type = ConditionType.Equal;
            CollectionNameCondition condition2 = new CollectionNameCondition();
            condition2.Key = "BlockHeight";
            condition2.Value = "100000";
            condition2.Type = ConditionType.Equal;
            conditions.Add(condition1);
            conditions.Add(condition2);
            List<string> indexNames = _blockIndexShardingKeyProvider.GetCollectionName(conditions);
            Assert.True(indexNames.First() == "aelfentitymappingtest.blockindex-aelf-"+100000/5);
        
        }
        [Fact]
        public void GetCollectionNameGreaterThan()
        {
            //GetCollectionNameForWriteTest();
            WriteBlockIndex();
            List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
            CollectionNameCondition condition1 = new CollectionNameCondition();
            condition1.Key = "ChainId";
            condition1.Value = "AELF";
            condition1.Type = ConditionType.Equal;
            CollectionNameCondition condition2 = new CollectionNameCondition();
            condition2.Key = "BlockHeight";
            condition2.Value = "1";
            condition2.Type = ConditionType.GreaterThan;
            conditions.Add(condition1);
            conditions.Add(condition2);
            List<string> indexNames = _blockIndexShardingKeyProvider.GetCollectionName(conditions);
            Assert.True(indexNames.Count == 3);
            DelBlockIndex();

        }
        [Fact]
        public void GetCollectionNameGreaterThanOrEqual()
        {
            // GetCollectionNameForWriteTest();
            WriteBlockIndex();
            List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
            CollectionNameCondition condition1 = new CollectionNameCondition();
            condition1.Key = "ChainId";
            condition1.Value = "AELF";
            condition1.Type = ConditionType.Equal;
            CollectionNameCondition condition2 = new CollectionNameCondition();
            condition2.Key = "BlockHeight";
            condition2.Value = "1";
            condition2.Type = ConditionType.GreaterThanOrEqual;
            conditions.Add(condition1);
            conditions.Add(condition2);
            List<string> indexNames = _blockIndexShardingKeyProvider.GetCollectionName(conditions);
            Assert.True(indexNames.Count == 3);
            DelBlockIndex();
        }
    
        [Fact]
        public void GetCollectionNameGreaterThanOrEqualAndLessThanOrEqual()
        {
            WriteBlockIndex();
            List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
            CollectionNameCondition condition1 = new CollectionNameCondition();
            condition1.Key = "ChainId";
            condition1.Value = "AELF";
            condition1.Type = ConditionType.Equal;
            CollectionNameCondition condition2 = new CollectionNameCondition();
            condition2.Key = "BlockHeight";
            condition2.Value = "1";
            condition2.Type = ConditionType.GreaterThanOrEqual;
            CollectionNameCondition condition3 = new CollectionNameCondition();
            condition3.Key = "BlockHeight";
            condition3.Value = "11";
            condition3.Type = ConditionType.LessThanOrEqual;
            conditions.Add(condition1);
            conditions.Add(condition2);
            conditions.Add(condition3);
            List<string> indexNames = _blockIndexShardingKeyProvider.GetCollectionName(conditions);
            Assert.True(indexNames.Count == 3);
            DelBlockIndex();

        }

        [Fact]
        public void GetCollectionNameByEntityList()
        {
            BlockIndex blockIndex01 = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 10,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            BlockIndex blockIndex02 = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 1000,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            BlockIndex blockIndex03 = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 2000,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            BlockIndex blockIndex04 = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 10000,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            List<BlockIndex> list = new List<BlockIndex>() { blockIndex01, blockIndex02, blockIndex03, blockIndex04 };
            var results = _blockIndexShardingKeyProvider.GetCollectionName(list);
            Assert.True(results.Count == 4);

        }
    }
}