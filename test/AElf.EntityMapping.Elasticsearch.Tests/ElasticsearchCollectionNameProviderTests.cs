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
        var collectionNames =  await _collectionNameProvider.GetFullCollectionNameAsync(null);
        collectionNames.Count.ShouldBe(0);
        
        var collectionNameCondition = new List<CollectionNameCondition>();
        collectionNameCondition.Add(new CollectionNameCondition
        {
            Key = nameof(BlockIndex.ChainId),
            Value = "AELF",
            Type = ConditionType.Equal
        });
        collectionNames = await _collectionNameProvider.GetFullCollectionNameAsync(collectionNameCondition);
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