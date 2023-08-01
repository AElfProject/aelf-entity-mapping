using AElf.BaseStorageMapper.Elasticsearch.Services;
using AElf.BaseStorageMapper.Sharding;
using Nest;

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
        
        // TODO: Add sharding support
        throw new NotImplementedException();
    }

    protected override string GetCollectionNameById<TKey>(TKey id)
    {
        // TODO: Add sharding support
        throw new NotImplementedException();
    }

    protected override string FormatCollectionName(string name)
    {
        return name.ToLower();
    }
}