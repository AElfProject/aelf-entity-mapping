using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Sharding;

namespace AElf.EntityMapping.Elasticsearch;

public class ElasticsearchCollectionNameProvider<TEntity> : ICollectionNameProvider<TEntity>
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

    public async Task<List<string>> GetFullCollectionNameAsync(List<CollectionNameCondition> conditions)
    {
        if (conditions == null || conditions.Count == 0)
            return new List<string> { GetDefaultCollectionName() };

        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
            return new List<string> { GetDefaultCollectionName() };
        
        var shardKeyCollectionNames = _shardingKeyProvider.GetCollectionName(conditions);
        var nonShardKeyCollectionNames =
            await _nonShardKeyRouteProvider.GetShardCollectionNameListByConditionsAsync(conditions);

        if (shardKeyCollectionNames.Count > 0 && nonShardKeyCollectionNames.Count > 0)
        {
            return shardKeyCollectionNames.Intersect(nonShardKeyCollectionNames).ToList();
        }

        return shardKeyCollectionNames.Concat(nonShardKeyCollectionNames).ToList();
    }

    public async Task<List<string>> GetFullCollectionNameByEntityAsync(TEntity entity)
    {
        if (entity == null)
            return new List<string> { GetDefaultCollectionName() };

        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
            return new List<string> { GetDefaultCollectionName() };
        var shardKeyCollectionName = _shardingKeyProvider.GetCollectionName(entity);
        return new List<string>() { shardKeyCollectionName };
    }

    public async Task<List<string>> GetFullCollectionNameByEntityAsync(List<TEntity> entitys)
    {
        if (entitys == null || entitys.Count == 0)
            return new List<string> { GetDefaultCollectionName() };

        return _elasticIndexService.IsShardingCollection(typeof(TEntity))
            ? _shardingKeyProvider.GetCollectionName(entitys)
            : new List<string> { GetDefaultCollectionName() };
    }

    public async Task<string> GetFullCollectionNameByIdAsync<TKey>(TKey id)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity))) 
            return GetDefaultCollectionName();
        return await _nonShardKeyRouteProvider.GetShardCollectionNameByIdAsync(id.ToString());
    }
}