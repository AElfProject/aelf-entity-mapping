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
    public IAbpLazyServiceProvider LazyServiceProvider { get; set; }
    private IElasticsearchRepository<RouteKeyCollection,string> _nonShardKeyRouteIndexRepository => LazyServiceProvider
        .LazyGetRequiredService<IElasticsearchRepository<RouteKeyCollection,string>>();
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;
    private List<CollectionRouteKeyItem<TEntity>> CollectionRouteKeys { get; set; }
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly ElasticsearchOptions _elasticsearchOptions;
    private readonly ILogger<CollectionRouteKeyProvider<TEntity>> _logger;

    public CollectionRouteKeyProvider(IElasticsearchClientProvider elasticsearchClientProvider,
        IShardingKeyProvider<TEntity> shardingKeyProvider,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions,
        IOptions<ElasticsearchOptions> elasticsearchOptions,
        ILogger<CollectionRouteKeyProvider<TEntity>> logger,
        IElasticIndexService elasticIndexService)
    {
        _elasticIndexService = elasticIndexService;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _shardingKeyProvider = shardingKeyProvider;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _elasticsearchOptions = elasticsearchOptions.Value;
        _logger = logger;

        InitializeNonShardKeys();
    }

    private void InitializeNonShardKeys()
    {
        if (CollectionRouteKeys == null)
        {
            CollectionRouteKeys = new List<CollectionRouteKeyItem<TEntity>>();
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
                    CollectionRouteKeys.Add(collectionRouteKeyItem);
                }
            }
            _logger.LogInformation($"NonShardKeyRouteProvider.InitializeNonShardKeys: _nonShardKeys: {JsonConvert.SerializeObject(CollectionRouteKeys.Select(n=>n.FieldName).ToList())}");
            
        }
    }

    public async Task<List<string>> GetCollectionNameAsync(
        List<CollectionNameCondition> conditions)
    {
        var collectionNameList = new List<string>();
        if (CollectionRouteKeys == null || CollectionRouteKeys.Count == 0)
        {
            return collectionNameList;
        }

        foreach (var condition in conditions)
        {
            var nonShardKey = CollectionRouteKeys.FirstOrDefault(f => f.FieldName == condition.Key);
            if (nonShardKey == null)
            {
                continue;
            }
            _logger.LogInformation($"NonShardKeyRouteProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                   $"nonShardKey: {JsonConvert.SerializeObject(nonShardKey.FieldName)}");

            if (condition.Value == null)
            {
                continue;
            }

            // var fieldValue = Convert.ChangeType(condition.Value, nonShardKey.FieldValueType);
            var fieldValue = condition.Value.ToString();
            var nonShardKeyRouteIndexName =
                IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), nonShardKey.FieldName,
                    _aelfEntityMappingOptions.CollectionPrefix);
            _logger.LogInformation($"NonShardKeyRouteProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                   $"nonShardKeyRouteIndexName: {nonShardKeyRouteIndexName}");
            if (condition.Type == ConditionType.Equal)
            {
                // ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
                // MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
                // ConstantExpression value = Expression.Constant(fieldValue);
                // BinaryExpression equals = Expression.Equal(field, value);
                // Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
                // var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
                var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(x => x.CollectionRouteKey == fieldValue,
                    nonShardKeyRouteIndexName);
                // var indexList =
                //     GetNonShardKeyRouteIndexListAsync(x => x.SearchKey == fieldValue, nonShardKeyRouteIndexName);

                _logger.LogInformation($"NonShardKeyRouteProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                       $"indexList: {JsonConvert.SerializeObject(indexList)}");
                var nameList = indexList.Select(x => x.CollectionName).Distinct().ToList();
                if(collectionNameList.Count == 0)
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
        if (CollectionRouteKeys == null || CollectionRouteKeys.Count == 0)
        {
            return collectionName;
        }
        
        var nonShardKey= CollectionRouteKeys[0];
        var nonShardKeyRouteIndexName = IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), nonShardKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
        // var routeIndex=await _nonShardKeyRouteIndexRepository.GetAsync(id, nonShardKeyRouteIndexName);
        var routeIndex = await GetCollectionRouteKeyIndexAsync(id, nonShardKeyRouteIndexName);
        if (routeIndex != null)
        {
            collectionName = routeIndex.CollectionName;
        }

        return collectionName;
    }

    public async Task<List<CollectionRouteKeyItem<TEntity>>> GetCollectionRouteKeysAsync()
    {
        return CollectionRouteKeys;
    }

    public async Task<RouteKeyCollection> GetCollectionRouteKeyIndexAsync(string id, string indexName, CancellationToken cancellationToken = default)
    {
        // return await _nonShardKeyRouteIndexRepository.GetAsync(id, indexName);

        var client = _elasticsearchClientProvider.GetClient();
        var selector = new Func<GetDescriptor<RouteKeyCollection>, IGetRequest>(s => s
            .Index(indexName));
        var result = new GetResponse<RouteKeyCollection>();
        result = await client.GetAsync(new Nest.DocumentPath<RouteKeyCollection>(new Id(new { id = id.ToString() })),
            selector, cancellationToken);
        return result.Found ? result.Source : null;
    }

    //TODO: move to non shard key route provider
    public async Task AddManyCollectionRouteKey(List<TEntity> modelList,List<string> fullIndexNameList, IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (CollectionRouteKeys!=null && CollectionRouteKeys.Any() && _shardingKeyProvider.IsShardingCollection())
        {
            foreach (var nonShardKey in CollectionRouteKeys)
            {
                var nonShardKeyRouteIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), nonShardKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var nonShardKeyRouteBulk = new BulkRequest(nonShardKeyRouteIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
                int indexNameCount = 0;
                foreach (var item in modelList)
                {
                    //TODO: use func to get value
                    // var value = item.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(item);
                    var value = nonShardKey.GetRouteKeyValueFunc(item);
                    string indexName = IndexNameHelper.RemoveCollectionPrefix(fullIndexNameList[indexNameCount],
                        _aelfEntityMappingOptions.CollectionPrefix);
                    var nonShardKeyRouteIndexModel = new RouteKeyCollection()
                    {
                        Id = item.Id.ToString(),
                        CollectionName = indexName,
                        // SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType)
                        CollectionRouteKey = value?.ToString()
                    };
                    nonShardKeyRouteBulk.Operations.Add(
                        new BulkIndexOperation<RouteKeyCollection>(nonShardKeyRouteIndexModel));
                    indexNameCount++;
                }

                var nonShardKeyRouteResponse = await client.BulkAsync(nonShardKeyRouteBulk, cancellationToken);
            }
        }
    }

    public async Task AddCollectionRouteKey(TEntity model,string fullIndexName, IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (!_shardingKeyProvider.IsShardingCollection())
        {
            return;
        }

        string indexName =
            IndexNameHelper.RemoveCollectionPrefix(fullIndexName, _aelfEntityMappingOptions.CollectionPrefix);
        
        if (CollectionRouteKeys!=null && CollectionRouteKeys.Any())
        {
            foreach (var nonShardKey in CollectionRouteKeys)
            {
                // var value = model.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(model);
                var value = nonShardKey.GetRouteKeyValueFunc(model);
                var nonShardKeyRouteIndexModel = new RouteKeyCollection()
                {
                    Id = model.Id.ToString(),
                    CollectionName = indexName,
                    // SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType)
                    CollectionRouteKey = value?.ToString()
                };

                var nonShardKeyRouteIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), nonShardKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var nonShardKeyRouteResult = await client.IndexAsync(nonShardKeyRouteIndexModel,
                    ss => ss.Index(nonShardKeyRouteIndexName).Refresh(_elasticsearchOptions.Refresh),
                    cancellationToken);

            }
        }
    }

    public async Task UpdateCollectionRouteKey(TEntity model, IElasticClient client,
        CancellationToken cancellationToken = default)
    {
        if (!_shardingKeyProvider.IsShardingCollection())
        {
            return;
        }
        
        if (CollectionRouteKeys!=null && CollectionRouteKeys.Any())
        {
            foreach (var nonShardKey in CollectionRouteKeys)
            {
                var nonShardKeyRouteIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), nonShardKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var nonShardKeyRouteIndexId = model.Id.ToString();
                var nonShardKeyRouteIndexModel =
                    await GetCollectionRouteKeyIndexAsync(nonShardKeyRouteIndexId,
                        nonShardKeyRouteIndexName);
                // var nonShardKeyRouteIndexModel = GetAsync((TKey)Convert.ChangeType(nonShardKeyRouteIndexId, typeof(TKey)), nonShardKeyRouteIndexName)  as NonShardKeyRouteCollection;

                // var value = model.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(model);
                var value = nonShardKey.GetRouteKeyValueFunc(model);
                if (nonShardKeyRouteIndexModel != null && nonShardKeyRouteIndexModel.CollectionRouteKey != value?.ToString())
                {
                    // nonShardKeyRouteIndexModel.SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType);
                    nonShardKeyRouteIndexModel.CollectionRouteKey = value?.ToString();

                    var nonShardKeyRouteResult = await client.UpdateAsync(
                        DocumentPath<RouteKeyCollection>.Id(new Id(nonShardKeyRouteIndexModel)),
                        ss => ss.Index(nonShardKeyRouteIndexName).Doc(nonShardKeyRouteIndexModel).RetryOnConflict(3)
                            .Refresh(_elasticsearchOptions.Refresh),
                        cancellationToken);
                }
            }
        }
    }

    public async Task DeleteManyCollectionRouteKey(List<TEntity> modelList,IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (CollectionRouteKeys!=null && CollectionRouteKeys.Any() && _shardingKeyProvider.IsShardingCollection())
        {
            foreach (var nonShardKey in CollectionRouteKeys)
            {
                var nonShardKeyRouteIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), nonShardKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var nonShardKeyRouteBulk = new BulkRequest(nonShardKeyRouteIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
                foreach (var item in modelList)
                {
                    nonShardKeyRouteBulk.Operations.Add(new BulkDeleteOperation<RouteKeyCollection>(new Id(item)));
                }
                
                var nonShardKeyRouteResponse = await client.BulkAsync(nonShardKeyRouteBulk, cancellationToken);
            }
        }
    }

    public async Task DeleteCollectionRouteKey(string id, IElasticClient client,
        CancellationToken cancellationToken = default)
    {
        if (!_shardingKeyProvider.IsShardingCollection())
        {
            return;
        }
        if (CollectionRouteKeys!=null && CollectionRouteKeys.Any())
        {
            foreach (var nonShardKey in CollectionRouteKeys)
            {
                var nonShardKeyRouteIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), nonShardKey.FieldName,_aelfEntityMappingOptions.CollectionPrefix);
                var nonShardKeyRouteIndexId = id;
                var nonShardKeyRouteResult=await client.DeleteAsync(
                    new DeleteRequest(nonShardKeyRouteIndexName, new Id(new { id = nonShardKeyRouteIndexId.ToString() }))
                        { Refresh = _elasticsearchOptions.Refresh }, cancellationToken);
            }
        }
    }
}