using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Sharding;

namespace AElf.EntityMapping.Elasticsearch;

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

    protected override async Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions)
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

    protected override  async Task<List<string>> GetCollectionNameByEntityAsync(TEntity entity)
    {
        if (entity == null)
            return new List<string> { GetDefaultCollectionName() };

        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
            return new List<string> { GetDefaultCollectionName() };
        var shardKeyCollectionName = _shardingKeyProvider.GetCollectionName(entity);
        return new List<string>() { shardKeyCollectionName };
    }

    protected override async Task<List<string>> GetCollectionNameByEntityAsync(List<TEntity> entitys)
    {
        if (entitys == null || entitys.Count == 0)
            return new List<string> { GetDefaultCollectionName() };

        return _elasticIndexService.IsShardingCollection(typeof(TEntity))
            ? _shardingKeyProvider.GetCollectionName(entitys)
            : new List<string> { GetDefaultCollectionName() };
    }

    protected override async Task<string> GetCollectionNameByIdAsync<TKey>(TKey id)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity))) 
            return GetDefaultCollectionName();
        return await _nonShardKeyRouteProvider.GetShardCollectionNameByIdAsync(id.ToString());
    }

    protected override string FormatCollectionName(string name)
    {
        return name.ToLower();
    }

    public override async Task<string> RemoveCollectionPrefix(string fullCollectionName)
    {
        var collectionName = fullCollectionName;
        if (!string.IsNullOrWhiteSpace(AElfEntityMappingOptions.CollectionPrefix))
        {
            collectionName = collectionName.RemovePreFix($"{AElfEntityMappingOptions.CollectionPrefix}.".ToLower());
        }

        return collectionName;
    }
}