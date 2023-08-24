using AElf.EntityMapping.Elasticsearch.Entities;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Sharding;
using AElf.EntityMapping.TestBase;
using Volo.Abp.DependencyInjection;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch.Sharding
{
    public class ShardingKeyProviderTest : AElfEntityMappingTestBase<AElfElasticsearchTestsModule>, ITransientDependency
    {
        private readonly IShardingKeyProvider<BlockIndex> _blockIndexShardingKeyProvider;
        private readonly IShardingKeyProvider<BlockIndex> _blockIndexShardingKeyProvider2;
        private readonly IShardingKeyProvider<LogEventIndex> _logEventIndexShardingKeyProvider;
        private readonly IShardingKeyProvider<TransactionIndex> _logTransationIndexShardingKeyProvider;
        private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;
        private readonly IElasticsearchRepository<LogEventIndex, string> _elasticsearchRepositoryLogEventIndex;
        
        public ShardingKeyProviderTest()
        {
            _blockIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<BlockIndex>>();
            _blockIndexShardingKeyProvider2 = GetRequiredService<IShardingKeyProvider<BlockIndex>>();
            _logEventIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<LogEventIndex>>();
            _logTransationIndexShardingKeyProvider = GetRequiredService<IShardingKeyProvider<TransactionIndex>>();
            _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
            _elasticsearchRepositoryLogEventIndex = GetRequiredService<IElasticsearchRepository<LogEventIndex, string>>();
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
            List<ShardingKeyInfo<BlockIndex>> blockPropertyFuncs = _blockIndexShardingKeyProvider.GetShardKeyInfoList();
            Assert.True(blockPropertyFuncs != null && blockPropertyFuncs.Count == 2);
            Assert.True(blockPropertyFuncs[0].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(blockPropertyFuncs[0].ShardKeys[1].ShardKeyName == "BlockHeight");
            Assert.True(blockPropertyFuncs[1].ShardKeys[0].ShardKeyName == "ChainId");
            Assert.True(blockPropertyFuncs[1].ShardKeys[1].ShardKeyName == "BlockHeight");

            List<ShardingKeyInfo<BlockIndex>> blockPropertyFuncs2 =
                _blockIndexShardingKeyProvider2.GetShardKeyInfoList();
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
            List<ShardingKeyInfo<LogEventIndex>> eventPropertyFuncs =
                _logEventIndexShardingKeyProvider.GetShardKeyInfoList();
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
            List<ShardingKeyInfo<LogEventIndex>>
                propertyFuncs = _logEventIndexShardingKeyProvider.GetShardKeyInfoList();
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
            var blockIndex = new BlockIndex
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
            var blockIndex = new BlockIndex
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
            Assert.True(indexNames.First() == "blockindex-aelf-" + 10 / 5);
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

        /*[Fact]
         //1个ChainId
        public async Task GetCollectionNameByMultiShardKey()
        {
            {
                var blockIndex =  new BlockIndex
                {
                    Id = "block001",
                    BlockHash = "BlockHash001",
                    BlockHeight = 5,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 5,
                    ChainId = "AELF",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex =  new BlockIndex
                {
                    Id = "block002",
                    BlockHash = "BlockHash002",
                    BlockHeight = 10,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 20,
                    ChainId = "AELF",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex =  new BlockIndex
                {
                    Id = "block003",
                    BlockHash = "BlockHash003",
                    BlockHeight = 3,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 30,
                    ChainId = "tDVV",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex =  new BlockIndex
                {
                    Id = "block004",
                    BlockHash = "BlockHash004",
                    BlockHeight = 6,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 40,
                    ChainId = "tDVV",
                    Confirmed = false
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }

            
            List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
            CollectionNameCondition condition1 = new CollectionNameCondition();
            condition1.Key = "ChainId";
            condition1.Value = "AELF";
            condition1.Type = ConditionType.Equal;
            CollectionNameCondition condition2 = new CollectionNameCondition();
            condition2.Key = "BlockHeight";
            condition2.Value = "1";
            condition2.Type = ConditionType.Equal;
            CollectionNameCondition condition3 = new CollectionNameCondition();
            condition3.Key = "Confirmed";
            condition3.Value = "True";
            condition3.Type = ConditionType.GreaterThanOrEqual;
            conditions.Add(condition1);
            conditions.Add(condition2);
            conditions.Add(condition3);
            List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
            Assert.True(indexNames.Count == 1);
            Assert.True(indexNames.First() == "blockindex-aelf");
        }*/
        /*[Fact]
         //1个BlockHeight
        public async Task GetCollectionNameByMultiShardKey()
        {
            {
                var blockIndex =  new BlockIndex
                {
                    Id = "block001",
                    BlockHash = "BlockHash001",
                    BlockHeight = 5,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 5,
                    ChainId = "AELF",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex =  new BlockIndex
                {
                    Id = "block002",
                    BlockHash = "BlockHash002",
                    BlockHeight = 10,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 20,
                    ChainId = "AELF",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex =  new BlockIndex
                {
                    Id = "block003",
                    BlockHash = "BlockHash003",
                    BlockHeight = 5,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 30,
                    ChainId = "tDVV",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex =  new BlockIndex
                {
                    Id = "block004",
                    BlockHash = "BlockHash004",
                    BlockHeight = 10,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 40,
                    ChainId = "tDVV",
                    Confirmed = false
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }

            
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
            condition3.Key = "Confirmed";
            condition3.Value = "True";
            condition3.Type = ConditionType.Equal;
            conditions.Add(condition1);
            conditions.Add(condition2);
            conditions.Add(condition3);
            List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
            Assert.True(indexNames.Count == 2);
            Assert.True(indexNames.First() == "blockindex-0");
            Assert.True(indexNames.Last() == "blockindex-1");

        }*/
        /*[Fact]
         //ChainId + Confirm
       public async Task GetCollectionNameByMultiShardKey()
       {
           {
               var blockIndex =  new BlockIndex
               {
                   Id = "block001",
                   BlockHash = "BlockHash001",
                   BlockHeight = 5,
                   BlockTime = DateTime.Now.AddDays(-8),
                   LogEventCount = 5,
                   ChainId = "AELF",
                   Confirmed = true
               };
               await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
               Thread.Sleep(500);
           }
           {
               var blockIndex =  new BlockIndex
               {
                   Id = "block002",
                   BlockHash = "BlockHash002",
                   BlockHeight = 10,
                   BlockTime = DateTime.Now.AddDays(-8),
                   LogEventCount = 20,
                   ChainId = "AELF",
                   Confirmed = false
               };
               await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
               Thread.Sleep(500);
           }
           {
               var blockIndex =  new BlockIndex
               {
                   Id = "block003",
                   BlockHash = "BlockHash003",
                   BlockHeight = 5,
                   BlockTime = DateTime.Now.AddDays(-8),
                   LogEventCount = 30,
                   ChainId = "tDVV",
                   Confirmed = true
               };
               await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
               Thread.Sleep(500);
           }
           {
               var blockIndex =  new BlockIndex
               {
                   Id = "block004",
                   BlockHash = "BlockHash004",
                   BlockHeight = 10,
                   BlockTime = DateTime.Now.AddDays(-8),
                   LogEventCount = 40,
                   ChainId = "tDVV",
                   Confirmed = false
               };
               await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
               Thread.Sleep(500);
           }

           
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
           condition3.Key = "Confirmed";
           condition3.Value = "True";
           condition3.Type = ConditionType.Equal;
           conditions.Add(condition1);
           conditions.Add(condition2);
           conditions.Add(condition3);
           List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
           Assert.True(indexNames.Count == 1);
           Assert.True(indexNames.First() == "blockindex-aelf-true");

           condition3 = new CollectionNameCondition();
           condition3.Key = "Confirmed";
           condition3.Value = "False";
           condition3.Type = ConditionType.Equal;
           conditions = new List<CollectionNameCondition>();
           conditions.Add(condition1);
           conditions.Add(condition2);
           conditions.Add(condition3);
           indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
           Assert.True(indexNames.Count == 1);
           Assert.True(indexNames.Last() == "blockindex-aelf-false");

       }*/

        /*[Fact]
        //ChainId + Confirm + BlockHeight
        public async Task GetCollectionNameByMultiShardKey()
        {
            {
                var blockIndex = new BlockIndex
                {
                    Id = "block001",
                    BlockHash = "BlockHash001",
                    BlockHeight = 1,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 5,
                    ChainId = "AELF",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex = new BlockIndex
                {
                    Id = "block001",
                    BlockHash = "BlockHash001",
                    BlockHeight = 5,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 5,
                    ChainId = "AELF",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex = new BlockIndex
                {
                    Id = "block002",
                    BlockHash = "BlockHash002",
                    BlockHeight = 1,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 20,
                    ChainId = "AELF",
                    Confirmed = false
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex = new BlockIndex
                {
                    Id = "block002",
                    BlockHash = "BlockHash002",
                    BlockHeight = 5,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 20,
                    ChainId = "AELF",
                    Confirmed = false
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex = new BlockIndex
                {
                    Id = "block003",
                    BlockHash = "BlockHash003",
                    BlockHeight = 1,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 30,
                    ChainId = "tDVV",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex = new BlockIndex
                {
                    Id = "block003",
                    BlockHash = "BlockHash003",
                    BlockHeight = 5,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 30,
                    ChainId = "tDVV",
                    Confirmed = true
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex = new BlockIndex
                {
                    Id = "block004",
                    BlockHash = "BlockHash004",
                    BlockHeight = 1,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 40,
                    ChainId = "tDVV",
                    Confirmed = false
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }
            {
                var blockIndex = new BlockIndex
                {
                    Id = "block004",
                    BlockHash = "BlockHash004",
                    BlockHeight = 5,
                    BlockTime = DateTime.Now.AddDays(-8),
                    LogEventCount = 40,
                    ChainId = "tDVV",
                    Confirmed = false
                };
                await _elasticsearchRepository.AddOrUpdateAsync(blockIndex);
                Thread.Sleep(500);
            }

            {
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
                condition3.Key = "Confirmed";
                condition3.Value = "True";
                condition3.Type = ConditionType.Equal;
                conditions.Add(condition1);
                conditions.Add(condition2);
                conditions.Add(condition3);
                List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
                Assert.True(indexNames.Count == 2);
                Assert.True(indexNames.First() == "blockindex-aelf-true-0");
                Assert.True(indexNames.Last() == "blockindex-aelf-true-1");
            }
            {
                List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
                CollectionNameCondition condition1 = new CollectionNameCondition();
                condition1.Key = "ChainId";
                condition1.Value = "tDVV";
                condition1.Type = ConditionType.Equal;
                CollectionNameCondition condition2 = new CollectionNameCondition();
                condition2.Key = "BlockHeight";
                condition2.Value = "1";
                condition2.Type = ConditionType.GreaterThanOrEqual;
                CollectionNameCondition condition3 = new CollectionNameCondition();
                condition3.Key = "Confirmed";
                condition3.Value = "False";
                condition3.Type = ConditionType.Equal;
                conditions.Add(condition1);
                conditions.Add(condition2);
                conditions.Add(condition3);
                List<string> indexNames = await _blockIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
                Assert.True(indexNames.Count == 2);
                Assert.True(indexNames.First() == "blockindex-tdvv-false-0");
                Assert.True(indexNames.Last() == "blockindex-tdvv-false-1");
            }
        }*/
        
        /*[Fact]
         public async Task GetCollectionNameByShardFloorKey()
        {
            {
                var logEventIndex = new LogEventIndex()
                {
                    Id = "block001",
                    BlockHash = "BlockHash001",
                    BlockHeight = 1,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "AELF",
                    Confirmed = true
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }
            {
                var logEventIndex = new LogEventIndex
                {
                    Id = "block002",
                    BlockHash = "BlockHash001",
                    BlockHeight = 2,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "AELF",
                    Confirmed = true
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }
            {
                var logEventIndex = new LogEventIndex
                {
                    Id = "block003",
                    BlockHash = "BlockHash002",
                    BlockHeight = 3,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "AELF",
                    Confirmed = false
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }
            {
                var logEventIndex = new LogEventIndex
                {
                    Id = "block004",
                    BlockHash = "BlockHash002",
                    BlockHeight = 4,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "AELF",
                    Confirmed = false
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }
            {
                var logEventIndex = new LogEventIndex
                {
                    Id = "block005",
                    BlockHash = "BlockHash003",
                    BlockHeight = 5,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "tDVV",
                    Confirmed = true
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }
            {
                var logEventIndex = new LogEventIndex
                {
                    Id = "block006",
                    BlockHash = "BlockHash003",
                    BlockHeight = 6,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "tDVV",
                    Confirmed = true
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }
            {
                var logEventIndex = new LogEventIndex
                {
                    Id = "block007",
                    BlockHash = "BlockHash004",
                    BlockHeight = 7,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "tDVV",
                    Confirmed = false
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }
            {
                var logEventIndex = new LogEventIndex
                {
                    Id = "block008",
                    BlockHash = "BlockHash004",
                    BlockHeight = 7,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "tDVV",
                    Confirmed = false
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }
            {
                var logEventIndex = new LogEventIndex
                {
                    Id = "block009",
                    BlockHash = "BlockHash004",
                    BlockHeight = 9,
                    BlockTime = DateTime.Now.AddDays(-8),
                    ChainId = "tDVV",
                    Confirmed = false
                };
                await _elasticsearchRepositoryLogEventIndex.AddOrUpdateAsync(logEventIndex);
                Thread.Sleep(500);
            }

            {
                List<CollectionNameCondition> conditions = new List<CollectionNameCondition>();
                CollectionNameCondition condition1 = new CollectionNameCondition();
                condition1.Key = "BlockHeight";
                condition1.Value = "3";
                condition1.Type = ConditionType.GreaterThanOrEqual;
                CollectionNameCondition condition2 = new CollectionNameCondition();
                condition2.Key = "BlockHeight";
                condition2.Value = "6";
                condition2.Type = ConditionType.LessThanOrEqual;
                conditions.Add(condition1);
                conditions.Add(condition2);
                List<string> indexNames = await _logEventIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
                Assert.True(indexNames.Count == 2);
                Assert.True(indexNames.First() == "logeventindex-1");
                Assert.True(indexNames.Last() == "logeventindex-2");
                indexNames = await _logEventIndexShardingKeyProvider.GetCollectionNameAsync(conditions);
                Assert.True(indexNames.Count == 2);
                Assert.True(indexNames.First() == "logeventindex-1");
                Assert.True(indexNames.Last() == "logeventindex-2");
            }
            
        }*/
    }
}