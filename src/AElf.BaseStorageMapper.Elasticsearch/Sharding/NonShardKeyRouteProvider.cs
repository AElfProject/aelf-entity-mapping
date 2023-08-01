using System.Linq.Expressions;
using System.Net.NetworkInformation;
using AElf.BaseStorageMapper.Elasticsearch.Repositories;
using AElf.BaseStorageMapper.Elasticsearch.Services;
using AElf.BaseStorageMapper.Sharding;
using Volo.Abp.Caching;
using Volo.Abp.Threading;

namespace AElf.BaseStorageMapper.Elasticsearch.Sharding;

public class NonShardKeyRouteProvider<TEntity>:INonShardKeyRouteProvider<TEntity> where TEntity : class
{
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IDistributedCache<List<CollectionMarkField>> _indexMarkFieldCache;
    private readonly IElasticsearchRepository<NonShardKeyRouteCollection,string> _nonShardKeyRouteIndexRepository;
    private List<CollectionMarkField> _nonShardKeys;

    public NonShardKeyRouteProvider(IDistributedCache<List<CollectionMarkField>> indexMarkFieldCache,
        IElasticIndexService elasticIndexService,
        IElasticsearchRepository<NonShardKeyRouteCollection, string> nonShardKeyRouteIndexRepository)
    {
        _indexMarkFieldCache = indexMarkFieldCache;
        _elasticIndexService = elasticIndexService;
        _nonShardKeyRouteIndexRepository = nonShardKeyRouteIndexRepository;

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

            var fieldValue = Convert.ChangeType(condition.Value, nonShardKey.FieldValueType);
            var nonShardKeyRouteIndexName =
                _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
            if (condition.Type == ConditionType.Equal)
            {
                // 定义一个参数表达式，表示要查询的对象
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");

                // 定义一个成员访问表达式，表示要查询的字段
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);

                // 定义一个常量表达式，表示要查询的字段值
                ConstantExpression value = Expression.Constant(fieldValue);

                // 定义一个相等比较表达式，表示要查询的条件
                BinaryExpression equals = Expression.Equal(field, value);

                // 将比较表达式组合成一个 lambda 表达式
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
                // 定义一个参数表达式，表示要查询的对象
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");

                // 定义一个成员访问表达式，表示要查询的字段
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);

                // 定义一个常量表达式，表示要查询的字段值
                ConstantExpression value = Expression.Constant(fieldValue);

                // 定义一个相等比较表达式，表示要查询的条件
                BinaryExpression equals = Expression.GreaterThan(field, value);

                // 将比较表达式组合成一个 lambda 表达式
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
                // 定义一个参数表达式，表示要查询的对象
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");

                // 定义一个成员访问表达式，表示要查询的字段
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);

                // 定义一个常量表达式，表示要查询的字段值
                ConstantExpression value = Expression.Constant(fieldValue);

                // 定义一个相等比较表达式，表示要查询的条件
                BinaryExpression equals = Expression.GreaterThanOrEqual(field, value);

                // 将比较表达式组合成一个 lambda 表达式
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
                // 定义一个参数表达式，表示要查询的对象
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");

                // 定义一个成员访问表达式，表示要查询的字段
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);

                // 定义一个常量表达式，表示要查询的字段值
                ConstantExpression value = Expression.Constant(condition.Value);

                // 定义一个相等比较表达式，表示要查询的条件
                BinaryExpression equals = Expression.LessThan(field, value);

                // 将比较表达式组合成一个 lambda 表达式
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
                // 定义一个参数表达式，表示要查询的对象
                ParameterExpression parameter = Expression.Parameter(typeof(NonShardKeyRouteCollection), "x");

                // 定义一个成员访问表达式，表示要查询的字段
                MemberExpression field = Expression.PropertyOrField(parameter, nonShardKey.FieldName);

                // 定义一个常量表达式，表示要查询的字段值
                ConstantExpression value = Expression.Constant(fieldValue);

                // 定义一个相等比较表达式，表示要查询的条件
                BinaryExpression equals = Expression.LessThanOrEqual(field, value);

                // 将比较表达式组合成一个 lambda 表达式
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
        if (indexMarkFields == null)
        {
            throw new Exception($"{typeof(TEntity).Name} Index marked field cache not found.");
        }

        return indexMarkFields.FindAll(f => f.IsRouteKey).ToList();;
    }
    
    public async Task<NonShardKeyRouteCollection> GetNonShardKeyRouteIndexAsync(string id,string indexName)
    {
        return await _nonShardKeyRouteIndexRepository.GetAsync(id, indexName);
    }
}