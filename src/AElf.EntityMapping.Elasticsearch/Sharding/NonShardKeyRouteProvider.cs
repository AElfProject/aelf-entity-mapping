using System.Linq.Expressions;
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

public class NonShardKeyRouteProvider<TEntity>:INonShardKeyRouteProvider<TEntity> where TEntity : class, IEntity<string>
{
    public IAbpLazyServiceProvider LazyServiceProvider { get; set; }
    private IElasticsearchRepository<NonShardKeyRouteCollection,string> _nonShardKeyRouteIndexRepository => LazyServiceProvider
        .LazyGetRequiredService<IElasticsearchRepository<NonShardKeyRouteCollection,string>>();
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IDistributedCache<List<CollectionRouteKeyCacheItem>> _collectionRouteKeyCache;
    // private readonly ICollectionNameProvider<TEntity> _collectionNameProvider;
    // private readonly IElasticsearchRepository<NonShardKeyRouteCollection,string> _nonShardKeyRouteIndexRepository;
    public List<CollectionRouteKeyCacheItem> NonShardKeys { get; set; }
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    // private readonly IElasticsearchQueryableFactory<NonShardKeyRouteCollection> _elasticsearchQueryableFactory;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly ElasticsearchOptions _elasticsearchOptions;
    private readonly ILogger<NonShardKeyRouteProvider<TEntity>> _logger;

    public NonShardKeyRouteProvider(IDistributedCache<List<CollectionRouteKeyCacheItem>> collectionRouteKeyCache,
        // ICollectionNameProvider<TEntity> collectionNameProvider,
        IElasticsearchClientProvider elasticsearchClientProvider,
        // IElasticsearchQueryableFactory<NonShardKeyRouteCollection> elasticsearchQueryableFactory, 
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions,
        IOptions<ElasticsearchOptions> elasticsearchOptions,
        ILogger<NonShardKeyRouteProvider<TEntity>> logger,
        IElasticIndexService elasticIndexService)
    {
        _collectionRouteKeyCache = collectionRouteKeyCache;
        _elasticIndexService = elasticIndexService;
        // _collectionNameProvider = collectionNameProvider;
        // _elasticsearchQueryableFactory = elasticsearchQueryableFactory;
        // _nonShardKeyRouteIndexRepository = nonShardKeyRouteIndexRepository;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _elasticsearchOptions = elasticsearchOptions.Value;
        _logger = logger;

        InitializeNonShardKeys();
    }

    private void InitializeNonShardKeys()
    {
        if (NonShardKeys == null)
        {
            AsyncHelper.RunSync(async () =>
            {
                NonShardKeys = await GetNonShardKeysAsync();
                _logger.LogInformation($"NonShardKeyRouteProvider.InitializeNonShardKeys:  " +
                                       $"_nonShardKeys: {JsonConvert.SerializeObject(NonShardKeys)}");
            });
        }
    }

    public async Task<List<string>> GetCollectionNameAsync(
        List<CollectionNameCondition> conditions)
    {
        var collectionNameList = new List<string>();
        if (NonShardKeys == null || NonShardKeys.Count == 0)
        {
            return collectionNameList;
        }

        foreach (var condition in conditions)
        {
            var nonShardKey = NonShardKeys.FirstOrDefault(f => f.FieldName == condition.Key);
            _logger.LogInformation($"NonShardKeyRouteProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                   $"nonShardKey: {JsonConvert.SerializeObject(nonShardKey)}");
            
            if (nonShardKey == null)
            {
                continue;
            }

            if (condition.Value == null)
            {
                continue;
            }

            // var fieldValue = Convert.ChangeType(condition.Value, nonShardKey.FieldValueType);
            var fieldValue = condition.Value.ToString();
            var nonShardKeyRouteIndexName =
                _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
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
                var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(x => x.SearchKey == fieldValue,
                    nonShardKeyRouteIndexName);
                // var indexList =
                //     GetNonShardKeyRouteIndexListAsync(x => x.SearchKey == fieldValue, nonShardKeyRouteIndexName);

                _logger.LogInformation($"NonShardKeyRouteProvider.GetShardCollectionNameListByConditionsAsync:  " +
                                       $"indexList: {JsonConvert.SerializeObject(indexList)}");
                var nameList = indexList.Select(x => x.ShardCollectionName).Distinct().ToList();
                if(collectionNameList.Count == 0)
                {
                    collectionNameList.AddRange(nameList);
                }
                else
                {
                    collectionNameList = collectionNameList.Intersect(nameList).ToList();
                }
            }

            // if (condition.Type == ConditionType.GreaterThan)
            // {
            //     ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
            //     MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
            //     ConstantExpression value = Expression.Constant(fieldValue);
            //     BinaryExpression equals = Expression.GreaterThan(field, value);
            //     Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
            //     var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
            //     var nameList = indexList.Select(x => x.ShardCollectionName).Distinct().ToList();
            //     if(collectionNameList.Count == 0)
            //     {
            //         collectionNameList.AddRange(nameList);
            //     }
            //     else
            //     {
            //         collectionNameList = collectionNameList.Intersect(nameList).ToList();
            //     }
            // }
            
            // if (condition.Type == ConditionType.GreaterThanOrEqual)
            // {
            //     ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
            //     MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
            //     ConstantExpression value = Expression.Constant(fieldValue);
            //     BinaryExpression equals = Expression.GreaterThanOrEqual(field, value);
            //     Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
            //     var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
            //     var nameList = indexList.Select(x => x.ShardCollectionName).Distinct().ToList();
            //     if(collectionNameList.Count == 0)
            //     {
            //         collectionNameList.AddRange(nameList);
            //     }
            //     else
            //     {
            //         collectionNameList = collectionNameList.Intersect(nameList).ToList();
            //     }
            // }
            
            // if (condition.Type == ConditionType.LessThan)
            // {
            //     ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
            //     MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
            //     ConstantExpression value = Expression.Constant(condition.Value);
            //     BinaryExpression equals = Expression.LessThan(field, value);
            //     Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
            //     var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
            //     var nameList = indexList.Select(x => x.ShardCollectionName).Distinct().ToList();
            //     if(collectionNameList.Count == 0)
            //     {
            //         collectionNameList.AddRange(nameList);
            //     }
            //     else
            //     {
            //         collectionNameList = collectionNameList.Intersect(nameList).ToList();
            //     }
            // }
            
            // if (condition.Type == ConditionType.LessThanOrEqual)
            // {
            //     ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
            //     MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
            //     ConstantExpression value = Expression.Constant(fieldValue);
            //     BinaryExpression equals = Expression.LessThanOrEqual(field, value);
            //     Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
            //     var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
            //     var nameList = indexList.Select(x => x.ShardCollectionName).Distinct().ToList();
            //     if(collectionNameList.Count == 0)
            //     {
            //         collectionNameList.AddRange(nameList);
            //     }
            //     else
            //     {
            //         collectionNameList = collectionNameList.Intersect(nameList).ToList();
            //     }
            // }
        }

        return collectionNameList;
    }

    public async Task<string> GetCollectionNameAsync(string id)
    {
        var collectionName=string.Empty;
        if (NonShardKeys == null || NonShardKeys.Count == 0)
        {
            return collectionName;
        }
        
        var nonShardKey= NonShardKeys[0];
        var nonShardKeyRouteIndexName = _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
        // var routeIndex=await _nonShardKeyRouteIndexRepository.GetAsync(id, nonShardKeyRouteIndexName);
        var routeIndex = await GetNonShardKeyRouteIndexAsync(id, nonShardKeyRouteIndexName);
        if (routeIndex != null)
        {
            collectionName = routeIndex.ShardCollectionName;
        }

        return collectionName;
    }

    public async Task<List<CollectionRouteKeyCacheItem>> GetNonShardKeysAsync()
    {
        var collectionRouteKeyCacheKey = _elasticIndexService.GetCollectionRouteKeyCacheName(typeof(TEntity));
        var collectionRouteKeyCacheItems = await _collectionRouteKeyCache.GetAsync(collectionRouteKeyCacheKey);
        if (collectionRouteKeyCacheItems != null)
        {
            return collectionRouteKeyCacheItems;
            // return indexMarkFields.FindAll(f => f.IsRouteKey).ToList();
            // throw new Exception($"{typeof(TEntity).Name} Index marked field cache not found.");
        }

        return new List<CollectionRouteKeyCacheItem>();
    }

    public async Task<NonShardKeyRouteCollection> GetNonShardKeyRouteIndexAsync(string id, string indexName, CancellationToken cancellationToken = default)
    {
        // return await _nonShardKeyRouteIndexRepository.GetAsync(id, indexName);

        var client = _elasticsearchClientProvider.GetClient();
        var selector = new Func<GetDescriptor<NonShardKeyRouteCollection>, IGetRequest>(s => s
            .Index(indexName));
        var result = new GetResponse<NonShardKeyRouteCollection>();
        result = await client.GetAsync(new Nest.DocumentPath<NonShardKeyRouteCollection>(new Id(new { id = id.ToString() })),
            selector, cancellationToken);
        return result.Found ? result.Source : null;
    }

    // private List<NonShardKeyRouteCollection> GetNonShardKeyRouteIndexListAsync(Expression<Func<NonShardKeyRouteCollection, bool>> predicate,string indexName, CancellationToken cancellationToken = default)
    // {
    //     var client = _elasticsearchClientProvider.GetClient();
    //     var queryable = _elasticsearchQueryableFactory.Create(client, indexName);
    //     return queryable.Where(predicate).ToList();
    // }
    
    //TODO: move to non shard key route provider
    public async Task AddManyNonShardKeyRoute(List<TEntity> modelList,List<string> fullIndexNameList, IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (NonShardKeys!=null && NonShardKeys.Any() && _elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            foreach (var nonShardKey in NonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteBulk = new BulkRequest(nonShardKeyRouteIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
                int indexNameCount = 0;
                foreach (var item in modelList)
                {
                    //TODO: use func to get value
                    var value = item.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(item);
                    string indexName = IndexNameHelper.RemoveCollectionPrefix(fullIndexNameList[indexNameCount],
                        _aelfEntityMappingOptions.CollectionPrefix);
                    var nonShardKeyRouteIndexModel = new NonShardKeyRouteCollection()
                    {
                        Id = item.Id.ToString(),
                        ShardCollectionName = indexName,
                        // SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType)
                        SearchKey = value?.ToString()
                    };
                    nonShardKeyRouteBulk.Operations.Add(
                        new BulkIndexOperation<NonShardKeyRouteCollection>(nonShardKeyRouteIndexModel));
                    indexNameCount++;
                }

                var nonShardKeyRouteResponse = await client.BulkAsync(nonShardKeyRouteBulk, cancellationToken);
            }
        }
    }

    public async Task AddNonShardKeyRoute(TEntity model,string fullIndexName, IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            return;
        }

        string indexName =
            IndexNameHelper.RemoveCollectionPrefix(fullIndexName, _aelfEntityMappingOptions.CollectionPrefix);
        
        if (NonShardKeys!=null && NonShardKeys.Any())
        {
            foreach (var nonShardKey in NonShardKeys)
            {
                var value = model.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(model);

                var nonShardKeyRouteIndexModel = new NonShardKeyRouteCollection()
                {
                    Id = model.Id.ToString(),
                    ShardCollectionName = indexName,
                    // SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType)
                    SearchKey = value?.ToString()
                };

                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteResult = await client.IndexAsync(nonShardKeyRouteIndexModel,
                    ss => ss.Index(nonShardKeyRouteIndexName).Refresh(_elasticsearchOptions.Refresh),
                    cancellationToken);

            }
        }
    }

    public async Task UpdateNonShardKeyRoute(TEntity model, IElasticClient client,
        CancellationToken cancellationToken = default)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            return;
        }
        
        if (NonShardKeys!=null && NonShardKeys.Any())
        {
            foreach (var nonShardKey in NonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteIndexId = model.Id.ToString();
                var nonShardKeyRouteIndexModel =
                    await GetNonShardKeyRouteIndexAsync(nonShardKeyRouteIndexId,
                        nonShardKeyRouteIndexName);
                // var nonShardKeyRouteIndexModel = GetAsync((TKey)Convert.ChangeType(nonShardKeyRouteIndexId, typeof(TKey)), nonShardKeyRouteIndexName)  as NonShardKeyRouteCollection;

                var value = model.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(model);
                if (nonShardKeyRouteIndexModel != null && nonShardKeyRouteIndexModel.SearchKey != value?.ToString())
                {
                    // nonShardKeyRouteIndexModel.SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType);
                    nonShardKeyRouteIndexModel.SearchKey = value?.ToString();

                    var nonShardKeyRouteResult = await client.UpdateAsync(
                        DocumentPath<NonShardKeyRouteCollection>.Id(new Id(nonShardKeyRouteIndexModel)),
                        ss => ss.Index(nonShardKeyRouteIndexName).Doc(nonShardKeyRouteIndexModel).RetryOnConflict(3)
                            .Refresh(_elasticsearchOptions.Refresh),
                        cancellationToken);
                }
            }
        }
    }

    public async Task DeleteManyNonShardKeyRoute(List<TEntity> modelList,IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (NonShardKeys!=null && NonShardKeys.Any() && _elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            foreach (var nonShardKey in NonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteBulk = new BulkRequest(nonShardKeyRouteIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
                foreach (var item in modelList)
                {
                    nonShardKeyRouteBulk.Operations.Add(new BulkDeleteOperation<NonShardKeyRouteCollection>(new Id(item)));
                }
                
                var nonShardKeyRouteResponse = await client.BulkAsync(nonShardKeyRouteBulk, cancellationToken);
            }
        }
    }

    public async Task DeleteNonShardKeyRoute(string id, IElasticClient client,
        CancellationToken cancellationToken = default)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            return;
        }
        if (NonShardKeys!=null && NonShardKeys.Any())
        {
            foreach (var nonShardKey in NonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteIndexId = id;
                var nonShardKeyRouteResult=await client.DeleteAsync(
                    new DeleteRequest(nonShardKeyRouteIndexName, new Id(new { id = nonShardKeyRouteIndexId.ToString() }))
                        { Refresh = _elasticsearchOptions.Refresh }, cancellationToken);
            }
        }
    }
}