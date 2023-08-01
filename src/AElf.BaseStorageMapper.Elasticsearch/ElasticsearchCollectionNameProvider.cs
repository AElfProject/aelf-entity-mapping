using AElf.BaseStorageMapper.Elasticsearch.Services;
using AElf.BaseStorageMapper.Sharding;
using Nest;
using Volo.Abp.Threading;

namespace AElf.BaseStorageMapper.Elasticsearch;

public class ElasticsearchCollectionNameProvider<TEntity> : CollectionNameProviderBase<TEntity>
    where TEntity : class
{
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;
    private readonly INonShardKeyRouteProvider<TEntity> _nonShardKeyRouteProvider;

    public ElasticsearchCollectionNameProvider(IElasticIndexService elasticIndexService,
        IShardingKeyProvider<TEntity> shardingKeyProvider, INonShardKeyRouteProvider<TEntity> nonShardKeyRouteProvider)
    {
        _elasticIndexService = elasticIndexService;
        _shardingKeyProvider = shardingKeyProvider;
        _nonShardKeyRouteProvider = nonShardKeyRouteProvider;
    }

    private string GetDefaultCollectionName()
    {
        return _elasticIndexService.GetDefaultIndexName(typeof(TEntity));
    }
    
    protected override List<string> GetCollectionName(List<CollectionNameCondition> conditions)
    {
        if(conditions==null || conditions.Count==0)
            return new List<string>{GetDefaultCollectionName()};
        
        var shardKeyCollectionNames= new List<string>();
        var nonShardKeyCollectionNames= new List<string>();

        AsyncHelper.RunSync(async () =>
        {
            // shardKeyCollectionNames= await _shardingKeyProvider.GetCollectionName(conditions);
            nonShardKeyCollectionNames =
                await _nonShardKeyRouteProvider.GetShardCollectionNameListByConditionsAsync(conditions);
        });

        if (shardKeyCollectionNames.Count > 0 && nonShardKeyCollectionNames.Count > 0)
        {
            return shardKeyCollectionNames.Intersect(nonShardKeyCollectionNames).ToList();
        }

        return shardKeyCollectionNames.Concat(nonShardKeyCollectionNames).ToList();
    }

    protected override string GetCollectionNameById<TKey>(TKey id)
    {
        string collectionName = string.Empty;
        AsyncHelper.RunSync(async () =>
        {
            collectionName=await _nonShardKeyRouteProvider.GetShardCollectionNameByIdAsync(id.ToString());
        });
        return collectionName;
    }

    protected override string FormatCollectionName(string name)
    {
        return name.ToLower();
    }
}