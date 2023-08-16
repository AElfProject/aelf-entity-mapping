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
            List<ShardingKeyInfo<BlockIndex>> blockPropertyFuncs = _blockIndexShardingKeyProvider.GetShardingKeyByEntity();
            Assert.True(blockPropertyFuncs != null && blockPropertyFuncs.Count == 2);
            Assert.True(blockPropertyFuncs[0].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(blockPropertyFuncs[0].ShardKeys[1].ShardKeyName == "BlockHeight");
            Assert.True(blockPropertyFuncs[1].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(blockPropertyFuncs[1].ShardKeys[1].ShardKeyName == "BlockHeight");
        
            List<ShardingKeyInfo<BlockIndex>> blockPropertyFuncs2 = _blockIndexShardingKeyProvider2.GetShardingKeyByEntity();
            Assert.True(blockPropertyFuncs2 != null && blockPropertyFuncs2.Count == 2);
            Assert.True(blockPropertyFuncs2[0].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(blockPropertyFuncs2[0].ShardKeys[1].ShardKeyName == "BlockHeight");
            Assert.True(blockPropertyFuncs2[1].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(blockPropertyFuncs2[1].ShardKeys[1].ShardKeyName == "BlockHeight");
        
            LogEventIndex eventIndex = new LogEventIndex()
            {
                ChainId = "AELF",
                BlockHeight = 123,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            List<ShardingKeyInfo<LogEventIndex>> eventPropertyFuncs = _logEventIndexShardingKeyProvider.GetShardingKeyByEntity();
            Assert.True(eventPropertyFuncs != null && eventPropertyFuncs.Count == 2);
            Assert.True(eventPropertyFuncs[0].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(eventPropertyFuncs[0].ShardKeys[1].ShardKeyName == "BlockHeight");
            Assert.True(eventPropertyFuncs[1].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(eventPropertyFuncs[1].ShardKeys[1].ShardKeyName == "BlockHeight");

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
            List<ShardingKeyInfo<LogEventIndex>> propertyFuncs = _logEventIndexShardingKeyProvider.GetShardingKeyByEntity();
            Assert.True(propertyFuncs != null && propertyFuncs.Count == 2);
            Assert.True(propertyFuncs[0].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(propertyFuncs[0].ShardKeys[1].ShardKeyName == "BlockHeight");
            Assert.True(propertyFuncs[1].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(propertyFuncs[1].ShardKeys[1].ShardKeyName == "BlockHeight");
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
        public async Task GetCollectionNameTest()
        {
            await WriteBlockIndex();
            List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
            CollectionNameCondition condition1 = new CollectionNameCondition();
            condition1.Key = "ChainId";
            condition1.Value = "AELF";
            condition1.Type = ConditionType.Equal;
            CollectionNameCondition condition2 = new CollectionNameCondition();
            condition2.Key = "TransationId";
            condition2.Value = "10";
            condition1.Type = ConditionType.Equal;
            conditions.Add(condition1);
            conditions.Add(condition2);
            var blockIndexNameMain = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
            Assert.True(blockIndexNameMain.Count == 4);
            Assert.True(blockIndexNameMain.First().StartsWith("blockindex-aelf-"));
        }
        
        [Fact]
        public async Task GetCollectionNameForWriteTest()
        {
            BlockIndex blockIndex = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 5,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            var blockIndexNameMain = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(blockIndex);
            Assert.True(blockIndexNameMain.StartsWith("blockindex-aelf-"));
            blockIndex.BlockHeight = 10;
            _blockIndexShardingKeyProvider.GetCollectionNameAsync(blockIndex);

            BlockIndex blockIndex2 = new BlockIndex()
            {
                ChainId = "tDVV",
                BlockHeight = 5,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            var blockIndexNameSide = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(blockIndex2);
            Assert.True(blockIndexNameSide.StartsWith("blockindex-tdvv-"));
        }

        private async Task WriteBlockIndex()
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
            await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
            Thread.Sleep(500);
            
            blockIndex.BlockHeight = 5;
            blockIndex.BlockHash = "BlockHash005";
            blockIndex.Id = "block005";
            await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
            
            blockIndex.BlockHeight = 10;
            blockIndex.BlockHash = "BlockHash010";
            blockIndex.Id = "block010";
            
            await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
            blockIndex.BlockHeight = 15;
            blockIndex.BlockHash = "BlockHash010";
            blockIndex.Id = "block010";
            await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
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
            _elasticsearchRepository.DeleteAsync(blockIndex.Id);
            blockIndex.BlockHeight = 5;
            blockIndex.BlockHash = "BlockHash005";
            blockIndex.Id = "block005";
            _elasticsearchRepository.DeleteAsync(blockIndex.Id);
            blockIndex.BlockHeight = 10;
            blockIndex.BlockHash = "BlockHash010";
            blockIndex.Id = "block010";
            _elasticsearchRepository.DeleteAsync(blockIndex.Id);
        }


        [Fact]
        public async Task GetCollectionNameEqual()
        {
            await WriteBlockIndex();
            
            List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
            CollectionNameCondition condition1 = new CollectionNameCondition();
            condition1.Key = "ChainId";
            condition1.Value = "AELF";
            condition1.Type = ConditionType.Equal;
            CollectionNameCondition condition2 = new CollectionNameCondition();
            condition2.Key = "BlockHeight";
            condition2.Value = "10";
            condition2.Type = ConditionType.Equal;
            conditions.Add(condition1);
            conditions.Add(condition2);
            List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
            Assert.True(indexNames.First() == "blockindex-aelf-"+10/5);
        
        }
        [Fact]
        public async Task GetCollectionNameGreaterThan()
        {
            //GetCollectionNameForWriteTest();
            await WriteBlockIndex();
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
            List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
            Assert.True(indexNames.Count == 4);
            DelBlockIndex();

        }
        [Fact]
        public async Task GetCollectionNameGreaterThanOrEqual()
        {
            // GetCollectionNameForWriteTest();
            await WriteBlockIndex();
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
            List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
            Assert.True(indexNames.Count == 4);
            DelBlockIndex();
        }
    
        [Fact]
        public async Task GetCollectionNameGreaterThanOrEqualAndLessThanOrEqual()
        {
            await WriteBlockIndex();
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
            List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
            Assert.True(indexNames.Count == 3);
            DelBlockIndex();

        }

        [Fact]
        public async Task GetCollectionNameByEntityList()
        {
            BlockIndex blockIndex01 = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 0,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            BlockIndex blockIndex02 = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 1,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            BlockIndex blockIndex03 = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 5,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            BlockIndex blockIndex04 = new BlockIndex()
            {
                ChainId = "AELF",
                BlockHeight = 10,
                BlockHash = "0x000000000",
                BlockTime = DateTime.Now,
                Confirmed = true
            };
            List<BlockIndex> list = new List<BlockIndex>() { blockIndex01, blockIndex02, blockIndex03, blockIndex04 };
            var results = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(list);
            Assert.True(results.Count == 4);

        }

        [Fact]
        public void test()
        {
            string str = "aelfindexer.blockindex-aelf-1024";
            string[] strs = str.Split('-');
            var suffix = strs.Last();
            var prefix = str.Substring(0, str.Length - suffix.Length - 1);
            Assert.True(prefix == "aelfindexer.blockindex-aelf");
            Assert.True(suffix == "1024");
            
        }
    }
}