using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Repositories;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Sharding;
using Volo.Abp.Caching;
using Volo.Abp.DependencyInjection;
using Volo.Abp.Threading;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class NonShardKeyRouteProvider<TEntity>:INonShardKeyRouteProvider<TEntity> where TEntity : class
{
    public IAbpLazyServiceProvider LazyServiceProvider { get; set; }
    protected IElasticsearchRepository<NonShardKeyRouteCollection,string> _nonShardKeyRouteIndexRepository => LazyServiceProvider
        .LazyGetRequiredService<ElasticsearchRepository<NonShardKeyRouteCollection,string>>();
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IDistributedCache<List<CollectionMarkField>> _indexMarkFieldCache;
    // private readonly IElasticsearchRepository<NonShardKeyRouteCollection,string> _nonShardKeyRouteIndexRepository;
    private List<CollectionMarkField> _nonShardKeys;

    public NonShardKeyRouteProvider(IDistributedCache<List<CollectionMarkField>> indexMarkFieldCache,
        IElasticIndexService elasticIndexService)
    {
        _indexMarkFieldCache = indexMarkFieldCache;
        _elasticIndexService = elasticIndexService;
        // _nonShardKeyRouteIndexRepository = nonShardKeyRouteIndexRepository;

        InitializeNonShardKeys();
    }
    
    private void InitializeNonShardKeys()
    {
        if (_nonShardKeys == null)
        {
            AsyncHelper.RunSync(async () =>
            {
                _nonShardKeys = await GetNonShardKeysAsync();
            });
        }
    }

    public async Task<List<string>> GetShardCollectionNameListByConditionsAsync(
        List<CollectionNameCondition> conditions)
    {
        var collectionNameList = new List<string>();
        if (_nonShardKeys == null || _nonShardKeys.Count == 0)
        {
            return collectionNameList;
        }

        foreach (var condition in conditions)
        {
            var nonShardKey = _nonShardKeys.FirstOrDefault(f => f.FieldName == condition.Key);

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
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);
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
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);
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
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);
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
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);
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
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);
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

    public async Task<string> GetShardCollectionNameByIdAsync(string id)
    {
        var collectionName=string.Empty;
        if (_nonShardKeys == null || _nonShardKeys.Count == 0)
        {
            return collectionName;
        }
        
        var nonShardKey= _nonShardKeys[0];
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
    
    public async Task<NonShardKeyRouteCollection> GetNonShardKeyRouteIndexAsync(string id,string indexName)
    {
        return await _nonShardKeyRouteIndexRepository.GetAsync(id, indexName);
    }
}