using System.Linq.Expressions;
using System.Reflection;
using AElf.EntityMapping.Elasticsearch.Exceptions;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nest;
using Newtonsoft.Json;
using Volo.Abp.DependencyInjection;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class CollectionRouteKeyProvider<TEntity>:ICollectionRouteKeyProvider<TEntity> where TEntity : class, IEntity<string>
{
    private readonly IAbpLazyServiceProvider _lazyServiceProvider;
    // private IElasticsearchRepository<RouteKeyCollection,string> _collectionRouteKeyIndexRepository => LazyServiceProvider
    //     .LazyGetRequiredService<IElasticsearchRepository<RouteKeyCollection,string>>();
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;
    private List<CollectionRouteKeyItem<TEntity>> _collectionRouteKeys;
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly ElasticsearchOptions _elasticsearchOptions;
    private readonly ILogger<CollectionRouteKeyProvider<TEntity>> _logger;

    public CollectionRouteKeyProvider(IElasticsearchClientProvider elasticsearchClientProvider,
        IShardingKeyProvider<TEntity> shardingKeyProvider,
        IAbpLazyServiceProvider lazyServiceProvider,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions,
        IOptions<ElasticsearchOptions> elasticsearchOptions,
        ILogger<CollectionRouteKeyProvider<TEntity>> logger,
        IElasticIndexService elasticIndexService)
    {
        _elasticIndexService = elasticIndexService;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _shardingKeyProvider = shardingKeyProvider;
        _lazyServiceProvider= lazyServiceProvider;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _elasticsearchOptions = elasticsearchOptions.Value;
        _logger = logger;

        InitializeCollectionRouteKeys();
    }

    private void InitializeCollectionRouteKeys()
    {
        if (_collectionRouteKeys == null)
        {
            _collectionRouteKeys = new List<CollectionRouteKeyItem<TEntity>>();
            Type type = typeof(TEntity);
            var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            foreach (var property in properties)
            {
                var collectionRouteKeyItem = new CollectionRouteKeyItem<TEntity>()
                {
                    FieldName = property.Name,
                    CollectionName = type.Name
                };
                //Find the field with the CollectionRouteKeyAttribute annotation set
                CollectionRouteKeyAttribute routeKeyAttribute = (CollectionRouteKeyAttribute)Attribute.GetCustomAttribute(property, typeof(CollectionRouteKeyAttribute));
                if (routeKeyAttribute != null)
                {
                    // Creates a Func expression that gets the value of the property
                    var parameter = Expression.Parameter(type, "entity");
                    var propertyAccess = Expression.Property(parameter, property);
                    var getPropertyFunc = Expression.Lambda<Func<TEntity, string>>(propertyAccess, parameter).Compile();
                    collectionRouteKeyItem.GetRouteKeyValueFunc = getPropertyFunc;
                    _collectionRouteKeys.Add(collectionRouteKeyItem);
                }
            }
            // _logger.LogDebug($"CollectionRouteKeyProvider.InitializeCollectionRouteKeys: _collectionRouteKeys: {JsonConvert.SerializeObject(_collectionRouteKeys.Select(n=>n.FieldName).ToList())}");
            
        }
    }

    public async Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions)
    {
        var collectionNameList = new List<string>();
        if (_collectionRouteKeys == null || _collectionRouteKeys.Count == 0)
        {
            return collectionNameList;
        }

        foreach (var condition in conditions)
        {
            var collectionRouteKey = _collectionRouteKeys.FirstOrDefault(f => f.FieldName == condition.Key);
            if (collectionRouteKey == null)
            {
                continue;
            }

            // _logger.LogDebug($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  " +
            //                        $"collectionRouteKey: {JsonConvert.SerializeObject(collectionRouteKey.FieldName)}");

            if (condition.Value == null)
            {
                continue;
            }

            // var fieldValue = Convert.ChangeType(condition.Value, collectionRouteKey.FieldValueType);
            var fieldValue = condition.Value.ToString();
            var collectionRouteKeyIndexName =
                IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                    _aelfEntityMappingOptions.CollectionPrefix);
            _logger.LogDebug($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                   $"collectionRouteKeyIndexName: {collectionRouteKeyIndexName}");
            if (condition.Type == ConditionType.Equal)
            {
                if (_elasticsearchClientProvider == null)
                {
                    _logger.LogError($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  elasticsearchClientProvider is null");
                }
                var client = _elasticsearchClientProvider.GetClient();
                if (client == null)
                {
                    _logger.LogError($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  client is null");
                }
                var result = await client.SearchAsync<RouteKeyCollection>(s =>
                    s.Index(collectionRouteKeyIndexName).Size(10000).Query(q => q.Term(t => t.Field(f => f.CollectionRouteKey).Value(fieldValue)))
                        .Collapse(c => c.Field(f=>f.CollectionName)).Aggregations(a => a
                            .Cardinality("courseAgg", ca => ca.Field(f=>f.CollectionName))));
                if (result == null)
                {
                    _logger.LogError($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  result is null fieldValue:{fieldValue}");
                }
                if (!result.IsValid)
                {
                    if (result.ServerError == null || result.ServerError.Error == null || result.ServerError.Error.Reason == null)
                    {
                        _logger.LogError($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  result.ServerError is null result:{JsonConvert.SerializeObject(result)}");
                    }
                    var reason = result.ServerError?.Error?.Reason ?? "Unknown error";
                    throw new ElasticsearchException($"Search document failed at index {collectionRouteKeyIndexName} :{reason}");
                }

                if (result.Documents == null)
                {
                    _logger.LogError($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  result.Documents is null fieldValue:{fieldValue}");
                }
                var collectionList = result.Documents.ToList();
                _logger.LogDebug($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                 $"collectionList: {JsonConvert.SerializeObject(collectionList)}");
                var nameList = collectionList.Select(x => x.CollectionName).Distinct().ToList();
                if (collectionNameList.Count == 0)
                {
                    collectionNameList.AddRange(nameList);
                }
                else
                {
                    collectionNameList = collectionNameList.Intersect(nameList).ToList();
                }
            }
        }

        return collectionNameList;
    }

    public async Task<string> GetCollectionNameAsync(string id)
    {
        var collectionName=string.Empty;
        if (_collectionRouteKeys == null || _collectionRouteKeys.Count == 0)
        {
            return collectionName;
        }
        
        var collectionRouteKey= _collectionRouteKeys[0];
        var collectionRouteKeyIndexName = IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
        // var routeIndex=await _collectionRouteKeyIndexRepository.GetAsync(id, collectionRouteKeyIndexName);
        var routeIndex = await GetRouteKeyCollectionAsync(id, collectionRouteKeyIndexName);
        if (routeIndex != null)
        {
            collectionName = routeIndex.CollectionName;
        }

        return collectionName;
    }

    public Task<List<CollectionRouteKeyItem<TEntity>>> GetCollectionRouteKeyItemsAsync()
    {
        return Task.FromResult(_collectionRouteKeys);
    }

    public async Task<IRouteKeyCollection> GetRouteKeyCollectionAsync(string id, string indexName, CancellationToken cancellationToken = default)
    {
        var client = _elasticsearchClientProvider.GetClient();
        var selector = new Func<GetDescriptor<RouteKeyCollection>, IGetRequest>(s => s
            .Index(indexName));
        var result = new GetResponse<RouteKeyCollection>();
        try
        {
            result = await client.GetAsync(new Nest.DocumentPath<RouteKeyCollection>(new Id(new { id = id.ToString() })),
                selector, cancellationToken);
        }
        catch (Exception e)
        {
            throw new ElasticsearchException($"Get Document failed at index {indexName} id {id.ToString()}", e);
        }
        return result.Found ? result.Source : null;
    }

    

    public async Task AddCollectionRouteKeyAsync(TEntity model,string fullCollectionName,CancellationToken cancellationToken = default)
    {
        if (!_shardingKeyProvider.IsShardingCollection())
        {
            return;
        }

        string indexName =
            IndexNameHelper.RemoveCollectionPrefix(fullCollectionName, _aelfEntityMappingOptions.CollectionPrefix);
        
        if (_collectionRouteKeys!=null && _collectionRouteKeys.Any())
        {
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in _collectionRouteKeys)
            {
                // var value = model.GetType().GetProperty(collectionRouteKey.FieldName)?.GetValue(model);
                var value = collectionRouteKey.GetRouteKeyValueFunc(model);
                var collectionRouteKeyIndexModel = new RouteKeyCollection()
                {
                    Id = model.Id.ToString(),
                    CollectionName = indexName,
                    // SearchKey = Convert.ChangeType(value, collectionRouteKey.FieldValueType)
                    CollectionRouteKey = value?.ToString()
                };

                var collectionRouteKeyIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var collectionRouteKeyResult = await client.IndexAsync(collectionRouteKeyIndexModel,
                    ss => ss.Index(collectionRouteKeyIndexName).Refresh(_elasticsearchOptions.Refresh),
                    cancellationToken);
                if (!collectionRouteKeyResult.IsValid)
                {
                    throw new ElasticsearchException(
                        $"Index document failed at index {collectionRouteKeyIndexName} id {(collectionRouteKeyIndexModel == null ? "" : collectionRouteKeyIndexModel.Id)} :" +
                        collectionRouteKeyResult.ServerError.Error.Reason);
                }

            }
        }
    }

    public async Task UpdateCollectionRouteKeyAsync(TEntity model, CancellationToken cancellationToken = default)
    {
        if (!_shardingKeyProvider.IsShardingCollection())
        {
            return;
        }
        
        if (_collectionRouteKeys!=null && _collectionRouteKeys.Any())
        {
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in _collectionRouteKeys)
            {
                var collectionRouteKeyIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var collectionRouteKeyIndexId = model.Id.ToString();
                var collectionRouteKeyIndexModel =
                    await GetRouteKeyCollectionAsync(collectionRouteKeyIndexId,
                        collectionRouteKeyIndexName, cancellationToken);
                // var collectionRouteKeyIndexModel = GetAsync((TKey)Convert.ChangeType(collectionRouteKeyIndexId, typeof(TKey)), collectionRouteKeyIndexName)  as RouteKeyCollection;

                // var value = model.GetType().GetProperty(collectionRouteKey.FieldName)?.GetValue(model);
                var value = collectionRouteKey.GetRouteKeyValueFunc(model);
                if (collectionRouteKeyIndexModel != null && collectionRouteKeyIndexModel.CollectionRouteKey != value?.ToString())
                {
                    // collectionRouteKeyIndexModel.SearchKey = Convert.ChangeType(value, collectionRouteKey.FieldValueType);
                    collectionRouteKeyIndexModel.CollectionRouteKey = value?.ToString();

                    var collectionRouteKeyResult = await client.UpdateAsync(
                        DocumentPath<RouteKeyCollection>.Id(new Id(collectionRouteKeyIndexModel)),
                        ss => ss.Index(collectionRouteKeyIndexName).Doc((RouteKeyCollection)collectionRouteKeyIndexModel).RetryOnConflict(3)
                            .Refresh(_elasticsearchOptions.Refresh),
                        cancellationToken);
                    if (!collectionRouteKeyResult.IsValid)
                    {
                        throw new ElasticsearchException(
                            $"Update document failed at index {collectionRouteKeyIndexName} id {(collectionRouteKeyIndexModel == null ? "" : collectionRouteKeyIndexModel.Id)} :" +
                            collectionRouteKeyResult.ServerError.Error.Reason);
                    }
                }
            }
        }
    }

    public async Task DeleteCollectionRouteKeyAsync(string id, CancellationToken cancellationToken = default)
    {
        if (!_shardingKeyProvider.IsShardingCollection())
        {
            return;
        }
        if (_collectionRouteKeys!=null && _collectionRouteKeys.Any())
        {
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in _collectionRouteKeys)
            {
                var collectionRouteKeyIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var collectionRouteKeyIndexId = id;
                var collectionRouteKeyResult=await client.DeleteAsync(
                    new DeleteRequest(collectionRouteKeyIndexName, new Id(new { id = collectionRouteKeyIndexId.ToString() }))
                        { Refresh = _elasticsearchOptions.Refresh }, cancellationToken);
                if (collectionRouteKeyResult.ServerError != null)
                {
                    throw new ElasticsearchException(
                        $"Delete document failed at index {collectionRouteKeyIndexName} id {collectionRouteKeyIndexId} :" +
                        collectionRouteKeyResult.ServerError.Error.Reason);
                }
            }
        }
    }
    
    private Task<IElasticClient> GetElasticsearchClientAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_elasticsearchClientProvider.GetClient());
    }
}