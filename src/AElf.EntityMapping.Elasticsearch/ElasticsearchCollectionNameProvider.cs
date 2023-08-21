using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;

namespace AElf.EntityMapping.Elasticsearch;

public class ElasticsearchCollectionNameProvider<TEntity> : CollectionNameProviderBase<TEntity>
    where TEntity : class
{
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;
    private readonly ICollectionRouteKeyProvider<TEntity> _collectionRouteKeyProvider;
    private readonly AElfEntityMappingOptions _entityMappingOptions;
    private readonly ILogger<ElasticsearchCollectionNameProvider<TEntity>> _logger;

    public ElasticsearchCollectionNameProvider(IShardingKeyProvider<TEntity> shardingKeyProvider,
        IElasticIndexService elasticIndexService,
        ICollectionRouteKeyProvider<TEntity> collectionRouteKeyProvider,
        IOptions<AElfEntityMappingOptions> entityMappingOptions,
        ILogger<ElasticsearchCollectionNameProvider<TEntity>> logger)
    {
        _elasticIndexService = elasticIndexService;
        _shardingKeyProvider = shardingKeyProvider;
        _collectionRouteKeyProvider = collectionRouteKeyProvider;
        _entityMappingOptions = entityMappingOptions.Value;
        _logger = logger;
    }

    private string GetDefaultCollectionName()
    {
        return IndexNameHelper.GetDefaultIndexName(typeof(TEntity));
    }

    protected override async Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions)
    {
        _logger.LogInformation("ElasticsearchCollectionNameProvider.GetCollectionNameAsync: conditions:{conditions}, IsShardingCollection:{IsShardingCollection}",JsonConvert.SerializeObject(conditions), !_shardingKeyProvider.IsShardingCollection());
        if (!_shardingKeyProvider.IsShardingCollection())
            return new List<string> { GetDefaultCollectionName() };
        
        var shardKeyCollectionNames = await _shardingKeyProvider.GetCollectionNameAsync(conditions);
        var routeKeyCollectionNames =
            await _collectionRouteKeyProvider.GetCollectionNameAsync(conditions);

        if (shardKeyCollectionNames.Count > 0 && routeKeyCollectionNames.Count > 0)
        {
            _logger.LogInformation("ElasticsearchCollectionNameProvider.GetCollectionNameAsync1:conditions:{conditions},shardKeyCollectionNames: {shardKeyCollectionNames},routeKeyCollectionNames:{routeKeyCollectionNames}",
                JsonConvert.SerializeObject(conditions),JsonConvert.SerializeObject(shardKeyCollectionNames),JsonConvert.SerializeObject(routeKeyCollectionNames));

            return shardKeyCollectionNames.Intersect(routeKeyCollectionNames).ToList();
        }
        return shardKeyCollectionNames.Concat(routeKeyCollectionNames).ToList();
    }

    protected override  async Task<List<string>> GetCollectionNameByEntityAsync(TEntity entity)
    {
        if (entity == null)
            return new List<string> { GetDefaultCollectionName() };

        if (!_shardingKeyProvider.IsShardingCollection())
            return new List<string> { GetDefaultCollectionName() };
        var shardKeyCollectionName = await _shardingKeyProvider.GetCollectionNameAsync(entity);
        return new List<string>() { shardKeyCollectionName };
    }

    protected override async Task<List<string>> GetCollectionNameByEntityAsync(List<TEntity> entities)
    {
        if (entities == null || entities.Count == 0)
            return new List<string> { GetDefaultCollectionName() };
        
        return _shardingKeyProvider.IsShardingCollection()
            ? await _shardingKeyProvider.GetCollectionNameAsync(entities)
            : new List<string> { GetDefaultCollectionName() };
    }

    protected override async Task<string> GetCollectionNameByIdAsync<TKey>(TKey id)
    {
        if (!_shardingKeyProvider.IsShardingCollection()) 
            return GetDefaultCollectionName();
        return await _collectionRouteKeyProvider.GetCollectionNameAsync(id.ToString());
    }

    protected override string FormatCollectionName(string name)
    {
        return name.ToLower();
    }
}