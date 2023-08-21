using AElf.EntityMapping.Elasticsearch.Entities;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Sharding;
using AElf.EntityMapping.TestBase;
using Volo.Abp.DependencyInjection;
using Xunit;

namespace AElf.EntityMapping.Elasticsearch.Sharding
{
    public class ShardingCollectionTailProviderTest : AElfEntityMappingTestBase<AElfElasticsearchTestsModule> ,ITransientDependency
    {
        private readonly IShardingCollectionTailProvider<BlockIndex> _blockIndexShardingKeyTailProvider;
        private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;
        
        public ShardingCollectionTailProviderTest()
        {
            _blockIndexShardingKeyTailProvider = GetRequiredService<IShardingCollectionTailProvider<BlockIndex>>();
            _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
        }
        
        [Fact]
        public async Task AddGetShardingCollectionTailAsyncTest()
        {
            await _blockIndexShardingKeyTailProvider.AddShardingCollectionTailAsync("aelf", 10);
            await _blockIndexShardingKeyTailProvider.AddShardingCollectionTailAsync("tdvv", 20);
            var aelfTail = await _blockIndexShardingKeyTailProvider.GetShardingCollectionTailAsync("aelf");
            var tdvvTail = await _blockIndexShardingKeyTailProvider.GetShardingCollectionTailAsync("tdvv");
            Assert.True(aelfTail == 10);
            Assert.True(tdvvTail == 20);
            
            tdvvTail = await _blockIndexShardingKeyTailProvider.GetShardingCollectionTailAsync("tdvv");
            Assert.True(tdvvTail == 20);
            
            await _blockIndexShardingKeyTailProvider.AddShardingCollectionTailAsync("tdvv", 30);
            tdvvTail = await _blockIndexShardingKeyTailProvider.GetShardingCollectionTailAsync("tdvv");
            Assert.True(tdvvTail == 30);

        }
    }
}