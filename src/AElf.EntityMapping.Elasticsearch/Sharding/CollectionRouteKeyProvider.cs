using System.Linq.Expressions;
using System.Reflection;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nest;
using Newtonsoft.Json;
using Volo.Abp.Caching;
using Volo.Abp.DependencyInjection;
using Volo.Abp.Domain.Entities;
using Volo.Abp.Threading;

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
            _logger.LogInformation($"CollectionRouteKeyProvider.InitializeCollectionRouteKeys: _collectionRouteKeys: {JsonConvert.SerializeObject(_collectionRouteKeys.Select(n=>n.FieldName).ToList())}");
            
        }
    }

    public async Task<List<string>> GetCollectionNameAsync(
        List<CollectionNameCondition> conditions, List<string> shardingKeyProviderCollectionNames = null)
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

            _logger.LogInformation($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                   $"collectionRouteKey: {JsonConvert.SerializeObject(collectionRouteKey.FieldName)}");

            if (condition.Value == null)
            {
                continue;
            }

            // var fieldValue = Convert.ChangeType(condition.Value, collectionRouteKey.FieldValueType);
            var fieldValue = condition.Value.ToString();
            var collectionRouteKeyIndexName =
                IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                    _aelfEntityMappingOptions.CollectionPrefix);
            _logger.LogInformation($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                   $"collectionRouteKeyIndexName: {collectionRouteKeyIndexName}");
            if (condition.Type == ConditionType.Equal)
            {
                
                var collectionList = new List<RouteKeyCollection>();
                if (shardingKeyProviderCollectionNames != null && shardingKeyProviderCollectionNames.Count > 0)
                {
                    // var parameter = Expression.Parameter(typeof(RouteKeyCollection), "x");
                    // var fieldValueConstantExpression = Expression.Constant(fieldValue);
                    // var propertyCollectionRouteKey = Expression.Property(parameter, "CollectionRouteKey");
                    // var equalsToValue1 = Expression.Equal(propertyCollectionRouteKey, fieldValueConstantExpression);
                    //
                    // // List<string> values = new List<string> { "value2", "value3", "value4", "value5", "value6" };
                    // Expression orExpression = null;
                    // var propertyCollectionName = Expression.Property(parameter, "CollectionName");
                    //
                    // foreach (var value in shardingKeyProviderCollectionNames)
                    // {
                    //     var constant = Expression.Constant(value);
                    //     var equalsToValue = Expression.Equal(propertyCollectionName, constant);
                    //
                    //     orExpression = orExpression == null ? equalsToValue : Expression.Or(orExpression, equalsToValue);
                    // }
                    //
                    // // create a expression (x.A == value1 && (x.B == value2 || x.B == value3 || x.B == value4 || x.B == value5 || x.B == value6))
                    // var andExpression = Expression.AndAlso(equalsToValue1, orExpression);
                    //
                    // // create a lambda expression (x => x.A == value1 && (x.B == value2 || x.B == value3 || x.B == value4 || x.B == value5 || x.B == value6))
                    // var lambda = Expression.Lambda<Func<RouteKeyCollection, bool>>(andExpression, parameter);
                    //
                    // collectionList = await _collectionRouteKeyIndexRepository.GetListAsync(
                    //     lambda,
                    //     collectionRouteKeyIndexName);
                    
                    var client = _elasticsearchClientProvider.GetClient();
                    var mustQuery = new List<Func<QueryContainerDescriptor<RouteKeyCollection>, QueryContainer>>();
                    mustQuery.Add(q => q.Term(i => i.Field(f => f.CollectionRouteKey).Value(fieldValue)));
                    var shouldQuery = new List<Func<QueryContainerDescriptor<RouteKeyCollection>, QueryContainer>>();
                    foreach (var value in shardingKeyProviderCollectionNames)
                    {
                        shouldQuery.Add(q => q.Term(i => i.Field(f => f.CollectionName).Value(value)));
                    }
                    mustQuery.Add(q => q.Bool(b => b.Should(shouldQuery)));
                    QueryContainer Filter(QueryContainerDescriptor<RouteKeyCollection> f) => f.Bool(b => b.Must(mustQuery));
                    Func<SearchDescriptor<RouteKeyCollection>, ISearchRequest> selector = null;
                    selector = new Func<SearchDescriptor<RouteKeyCollection>, ISearchRequest>(s =>
                        s.Index(collectionRouteKeyIndexName).Query(Filter).From(0).Size(10000));
                    var result = await client.SearchAsync(selector);
                    if (!result.IsValid)
                    {
                        throw new Exception($"Search document failed at index {collectionRouteKeyIndexName} :" + result.ServerError.Error.Reason);
                    }
                    collectionList = result.Documents.ToList();
                }
                else
                {
                    var _collectionRouteKeyIndexRepository = _lazyServiceProvider
                        .LazyGetRequiredService<IElasticsearchRepository<RouteKeyCollection, string>>();
                    collectionList = await _collectionRouteKeyIndexRepository.GetListAsync(
                        x => x.CollectionRouteKey == fieldValue,
                        collectionRouteKeyIndexName);
                }
                _logger.LogInformation($"CollectionRouteKeyProvider.GetShardCollectionNameListByConditionsAsync:  " +
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
        result = await client.GetAsync(new Nest.DocumentPath<RouteKeyCollection>(new Id(new { id = id.ToString() })),
            selector, cancellationToken);
        return result.Found ? result.Source : null;
    }
    
    public async Task AddManyCollectionRouteKeyAsync(List<TEntity> modelList,List<string> fullCollectionNameList,CancellationToken cancellationToken = default)
    {
        if (_collectionRouteKeys!=null && _collectionRouteKeys.Any() && _shardingKeyProvider.IsShardingCollection())
        {
            var routeKeyTaskList = new List<Task>();
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in _collectionRouteKeys)
            {
                routeKeyTaskList.Add(BulkAddRouteKey(client,modelList,collectionRouteKey,fullCollectionNameList,cancellationToken));
            }
            await Task.WhenAll(routeKeyTaskList.ToArray());
        }
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
                        collectionRouteKeyIndexName);
                // var collectionRouteKeyIndexModel = GetAsync((TKey)Convert.ChangeType(collectionRouteKeyIndexId, typeof(TKey)), collectionRouteKeyIndexName)  as RouteKeyCollection;

                // var value = model.GetType().GetProperty(collectionRouteKey.FieldName)?.GetValue(model);
                var value = collectionRouteKey.GetRouteKeyValueFunc(model);
                if (collectionRouteKeyIndexModel != null && collectionRouteKeyIndexModel.CollectionRouteKey != value?.ToString())
                {
                    // collectionRouteKeyIndexModel.SearchKey = Convert.ChangeType(value, collectionRouteKey.FieldValueType);
                    collectionRouteKeyIndexModel.CollectionRouteKey = value?.ToString();

                    var collectionRouteKeyRouteResult = await client.UpdateAsync(
                        DocumentPath<RouteKeyCollection>.Id(new Id(collectionRouteKeyIndexModel)),
                        ss => ss.Index(collectionRouteKeyIndexName).Doc((RouteKeyCollection)collectionRouteKeyIndexModel).RetryOnConflict(3)
                            .Refresh(_elasticsearchOptions.Refresh),
                        cancellationToken);
                }
            }
        }
    }

    public async Task DeleteManyCollectionRouteKeyAsync(List<TEntity> modelList,CancellationToken cancellationToken = default)
    {
        if (_collectionRouteKeys!=null && _collectionRouteKeys.Any() && _shardingKeyProvider.IsShardingCollection())
        {
            var routeKeyTaskList = new List<Task>();
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in _collectionRouteKeys)
            {
                routeKeyTaskList.Add(BulkDeleteRouteKey(client,modelList,collectionRouteKey,cancellationToken));
            }
            await Task.WhenAll(routeKeyTaskList.ToArray());
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
                var collectionRouteKeyRouteIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var collectionRouteKeyIndexId = id;
                var collectionRouteKeyResult=await client.DeleteAsync(
                    new DeleteRequest(collectionRouteKeyRouteIndexName, new Id(new { id = collectionRouteKeyIndexId.ToString() }))
                        { Refresh = _elasticsearchOptions.Refresh }, cancellationToken);
            }
        }
    }
    
    private Task<IElasticClient> GetElasticsearchClientAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_elasticsearchClientProvider.GetClient());
    }

    private async Task BulkAddRouteKey(IElasticClient client, List<TEntity> modelList,
        CollectionRouteKeyItem<TEntity> collectionRouteKey, List<string> fullCollectionNameList,
        CancellationToken cancellationToken)
    {
        var collectionRouteKeyIndexName =
            IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                _aelfEntityMappingOptions.CollectionPrefix);
        var collectionRouteKeyBulk = new BulkRequest(collectionRouteKeyIndexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        int indexNameCount = 0;
        foreach (var item in modelList)
        {
            // var value = item.GetType().GetProperty(collectionRouteKey.FieldName)?.GetValue(item);
            var value = collectionRouteKey.GetRouteKeyValueFunc(item);
            string indexName = IndexNameHelper.RemoveCollectionPrefix(fullCollectionNameList[indexNameCount],
                _aelfEntityMappingOptions.CollectionPrefix);
            var collectionRouteKeyIndexModel = new RouteKeyCollection()
            {
                Id = item.Id.ToString(),
                CollectionName = indexName,
                // SearchKey = Convert.ChangeType(value, collectionRouteKey.FieldValueType)
                CollectionRouteKey = value?.ToString()
            };
            collectionRouteKeyBulk.Operations.Add(
                new BulkIndexOperation<RouteKeyCollection>(collectionRouteKeyIndexModel));
            indexNameCount++;
        }

        await client.BulkAsync(collectionRouteKeyBulk, cancellationToken);
    }

    private async Task BulkDeleteRouteKey(IElasticClient client, List<TEntity> modelList,
        CollectionRouteKeyItem<TEntity> collectionRouteKey, CancellationToken cancellationToken)
    {
        var collectionRouteKeyRouteIndexName =
            IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                _aelfEntityMappingOptions.CollectionPrefix);
        var collectionRouteKeyRouteBulk = new BulkRequest(collectionRouteKeyRouteIndexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        foreach (var item in modelList)
        {
            collectionRouteKeyRouteBulk.Operations.Add(new BulkDeleteOperation<RouteKeyCollection>(new Id(item)));
        }

        await client.BulkAsync(collectionRouteKeyRouteBulk, cancellationToken);
    }
}