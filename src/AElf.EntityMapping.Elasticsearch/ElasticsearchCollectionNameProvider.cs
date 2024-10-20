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
    private readonly AElfEntityMappingOptions _entityMappingOptions;
    private readonly ILogger<ElasticsearchCollectionNameProvider<TEntity>> _logger;

    public ElasticsearchCollectionNameProvider(IShardingKeyProvider<TEntity> shardingKeyProvider,
        IElasticIndexService elasticIndexService,
        IOptions<AElfEntityMappingOptions> entityMappingOptions,
        ILogger<ElasticsearchCollectionNameProvider<TEntity>> logger)
    {
        _elasticIndexService = elasticIndexService;
        _shardingKeyProvider = shardingKeyProvider;
        _entityMappingOptions = entityMappingOptions.Value;
        _logger = logger;
    }

    private string GetDefaultCollectionName()
    {
        return IndexNameHelper.GetDefaultIndexName(typeof(TEntity));
    }

    protected override async Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions)
    {
        if (!_shardingKeyProvider.IsShardingCollection())
            return new List<string> { GetDefaultCollectionName() };

        var shardKeyCollectionNames = await _shardingKeyProvider.GetCollectionNameAsync(conditions);
        if (shardKeyCollectionNames.IsNullOrEmpty())
        {
            return new List<string> { GetDefaultCollectionName() };
        }

        return shardKeyCollectionNames;
    }

    protected override async Task<List<string>> GetCollectionNameByEntityAsync(TEntity entity)
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
        return GetDefaultCollectionName();
    }

    protected override string FormatCollectionName(string name)
    {
        return name.ToLower();
    }
}