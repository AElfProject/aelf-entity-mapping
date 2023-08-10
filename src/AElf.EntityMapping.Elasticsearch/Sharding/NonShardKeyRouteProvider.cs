using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Sharding;
using Nest;
using Volo.Abp.Caching;
using Volo.Abp.DependencyInjection;
using Volo.Abp.Threading;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class NonShardKeyRouteProvider<TEntity>:INonShardKeyRouteProvider<TEntity> where TEntity : class
{
    public IAbpLazyServiceProvider LazyServiceProvider { get; set; }
    protected IElasticsearchRepository<NonShardKeyRouteCollection,string> _nonShardKeyRouteIndexRepository => LazyServiceProvider
        .LazyGetRequiredService<IElasticsearchRepository<NonShardKeyRouteCollection,string>>();
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IDistributedCache<List<CollectionMarkField>> _indexMarkFieldCache;
    // private readonly IElasticsearchRepository<NonShardKeyRouteCollection,string> _nonShardKeyRouteIndexRepository;
    public List<CollectionMarkField> NonShardKeys { get; set; }
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;

    public NonShardKeyRouteProvider(IDistributedCache<List<CollectionMarkField>> indexMarkFieldCache,
        IElasticsearchClientProvider elasticsearchClientProvider,
        IElasticIndexService elasticIndexService)
    {
        _indexMarkFieldCache = indexMarkFieldCache;
        _elasticIndexService = elasticIndexService;
        // _nonShardKeyRouteIndexRepository = nonShardKeyRouteIndexRepository;
        _elasticsearchClientProvider = elasticsearchClientProvider;

        InitializeNonShardKeys();
    }
    
    private void InitializeNonShardKeys()
    {
        if (NonShardKeys == null)
        {
            AsyncHelper.RunSync(async () =>
            {
                NonShardKeys = await GetNonShardKeysAsync();
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

            if (nonShardKey == null)
            {
                continue;
            }

            if (condition.Value == null)
            {
                continue;
            }

            // var fieldValue = Convert.ChangeType(condition.Value, nonShardKey.FieldValueType);
            var fieldValue = condition.Value;
            var nonShardKeyRouteIndexName =
                _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
            if (condition.Type == ConditionType.Equal)
            {
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
                MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
                ConstantExpression value = Expression.Constant(fieldValue);
                BinaryExpression equals = Expression.Equal(field, value);
                Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
                var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
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

            if (condition.Type == ConditionType.GreaterThan)
            {
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
                MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
                ConstantExpression value = Expression.Constant(fieldValue);
                BinaryExpression equals = Expression.GreaterThan(field, value);
                Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
                var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
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
            
            if (condition.Type == ConditionType.GreaterThanOrEqual)
            {
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
                MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
                ConstantExpression value = Expression.Constant(fieldValue);
                BinaryExpression equals = Expression.GreaterThanOrEqual(field, value);
                Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
                var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
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
            
            if (condition.Type == ConditionType.LessThan)
            {
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
                MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
                ConstantExpression value = Expression.Constant(condition.Value);
                BinaryExpression equals = Expression.LessThan(field, value);
                Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
                var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
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
            
            if (condition.Type == ConditionType.LessThanOrEqual)
            {
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");
                MemberExpression field = Expression.PropertyOrField(parameter, nameof(NonShardKeyRouteCollection.SearchKey));
                ConstantExpression value = Expression.Constant(fieldValue);
                BinaryExpression equals = Expression.LessThanOrEqual(field, value);
                Expression<Func<NonShardKeyRouteCollection, bool>> lambda = Expression.Lambda<Func<NonShardKeyRouteCollection, bool>>(equals, parameter);
                var indexList = await _nonShardKeyRouteIndexRepository.GetListAsync(lambda, nonShardKeyRouteIndexName);
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
        var routeIndex=await _nonShardKeyRouteIndexRepository.GetAsync(id, nonShardKeyRouteIndexName);
        if (routeIndex != null)
        {
            collectionName = routeIndex.ShardCollectionName;
        }

        return collectionName;
    }

    public async Task<List<CollectionMarkField>> GetNonShardKeysAsync()
    {
        var indexMarkFieldsCacheKey = _elasticIndexService.GetIndexMarkFieldCacheName(typeof(TEntity));
        var indexMarkFields = await _indexMarkFieldCache.GetAsync(indexMarkFieldsCacheKey);
        if (indexMarkFields != null)
        {
            return indexMarkFields.FindAll(f => f.IsRouteKey).ToList();;
            // throw new Exception($"{typeof(TEntity).Name} Index marked field cache not found.");
        }

        return new List<CollectionMarkField>();
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
}