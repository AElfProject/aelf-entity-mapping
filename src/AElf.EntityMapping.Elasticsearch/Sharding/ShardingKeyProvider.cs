using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nest;
using Newtonsoft.Json;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class ShardingKeyProvider<TEntity> : IShardingKeyProvider<TEntity> where TEntity : class, IAElfEntity
{
    private readonly ElasticsearchOptions _indexSettingOptions;
    private readonly IElasticIndexService _elasticIndexService;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly List<ShardInitSetting> _indexShardOptions;
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ShardingKeyProvider<TEntity>> _logger;

    private InitShardType? _isShardIndex = null;
    private List<ShardingKeyInfo<TEntity>> ShardKeyInfoList = new List<ShardingKeyInfo<TEntity>>();
    private Dictionary<string, bool> _existIndexShardDictionary = new Dictionary<string, bool>();

    public ShardingKeyProvider(IOptions<ElasticsearchOptions> indexSettingOptions,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions, IElasticIndexService elasticIndexService,
        IElasticsearchClientProvider elasticsearchClientProvider,
        ILogger<ShardingKeyProvider<TEntity>> logger)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _indexShardOptions = aelfEntityMappingOptions.Value.ShardInitSettings;
        _elasticIndexService = elasticIndexService;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _logger = logger;
    }

    public ShardingKeyProvider()
    {
    }

    private bool CheckCollectionType(Type type)
    {
        var compareType = typeof(IAElfEntity);
        if (compareType.IsAssignableFrom(type) && !compareType.IsAssignableFrom(type.BaseType) &&
            !type.IsAbstract && type.IsClass && compareType != type)
        {
            return true;
        }

        return false;
    }

    public void SetShardingKey(int currentNo, int totalCount, string keyName, string step, int order, string value,
        StepType stepType, Expression body,
        ReadOnlyCollection<ParameterExpression> parameterExpressions)
    {
        var expression = Expression.Lambda<Func<TEntity, object>>(
            Expression.Convert(body, typeof(object)), parameterExpressions);
        var func = expression.Compile();
        if (ShardKeyInfoList.Count <= currentNo)
        {
            ShardKeyInfoList.Add(new ShardingKeyInfo<TEntity>(keyName, step.ToString(), order, value, stepType, func));
        }
        else
        {
            ShardKeyInfoList[currentNo].ShardKeys
                .Add(new ShardingKey<TEntity>(keyName, step, order, value, stepType, func));
        }
    }

    public List<ShardingKeyInfo<TEntity>> GetShardingKeyByEntity()
    {
        Type type = typeof(TEntity);
        if (_isShardIndex == null)
        {
            InitShardProvider();
        }

        return ShardKeyInfoList;
    }

    private async Task<long> GetShardCollectionMaxNoAsync(List<CollectionNameCondition> conditions)
    {
        ShardingCollectionTail shardingCollectionTail = new ShardingCollectionTail();
        shardingCollectionTail.EntityName = typeof(TEntity).Name;
        var result = await GetCollectionMaxShardIndexAsync(shardingCollectionTail);
        if (result is null || result.Item1 == 0)
        {
            return 0;
        }

        List<ShardingCollectionTail> catchList = result.Item2;
        List<ShardingKeyInfo<TEntity>> entitys = GetShardingKeyByEntity();
        if (entitys.IsNullOrEmpty())
        {
            return 0;
        }

        foreach (var condition in conditions)
        {
            var entity = entitys.Find(a =>
                a.ShardKeys.Find(b => b.ShardKeyName == condition.Key && b.StepType == StepType.None) != null);
            if (entity != null && catchList != null)
            {
                ShardingCollectionTail cacheDto =
                    catchList.Find(a => a.TailPrefix.StartsWith(condition.Value.ToString().ToLower()));
                if (cacheDto != null)
                {
                    return cacheDto.Tail;
                }
            }
        }

        return 0;
    }

    public async Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions)
    {
        var indexName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity)); 
        long min = 0;
        long max = await GetShardCollectionMaxNoAsync(conditions);
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionName:  " +
                               $"conditions: {JsonConvert.SerializeObject(conditions)},min:{min},max:{max}");
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardingKeyByEntity();

        if (shardingKeyInfos.IsNullOrEmpty())
        {
            return new List<string>() { indexName.ToLower() };
        }
        
        foreach (var shardingKeyInfo in shardingKeyInfos)
        {
            bool findGroup = false;
            foreach (var entity in shardingKeyInfo.ShardKeys)
            {
                if (entity.StepType == StepType.None)
                {
                    if (conditions.Find(a => a.Key == entity.ShardKeyName)?.Value.ToString() == entity.Value)
                    {
                        indexName = indexName + "-" + conditions.Find(a => a.Key == entity.ShardKeyName)!.Value ??
                                    throw new InvalidOleVariantTypeException();
                        findGroup = true;
                    }
                }
                else
                {
                    if(!findGroup) continue;
                    if (entity.StepType != StepType.Floor)
                    {
                        throw new Exception(entity.ShardKeyName + "need config StepType equal Floor");
                    }

                    var shardConditions = conditions.FindAll(a => a.Key == entity.ShardKeyName);
                    foreach (var condition in shardConditions)
                    {
                        var conditionType = condition.Type;
                        if (conditionType == ConditionType.Equal)
                        {
                            indexName = indexName + "-" +
                                        (int.Parse(conditions.Find(a => a.Key == entity.ShardKeyName).Value
                                                       .ToString() ??
                                                   throw new InvalidOperationException()) / int.Parse(entity.Step));
                            return new List<string>() { indexName.ToLower() };
                        }

                        if (conditionType == ConditionType.GreaterThan)
                        {
                            var value = (int.Parse(
                                conditions.Find(a =>
                                        a.Key == entity.ShardKeyName && a.Type == ConditionType.GreaterThan)
                                    .Value.ToString() ??
                                throw new InvalidOperationException()) / int.Parse(entity.Step));
                            min = ((value + 1) % int.Parse(entity.Step) == 0) ? value + 1 : value;
                        }

                        if (conditionType == ConditionType.GreaterThanOrEqual)
                        {
                            min = (int.Parse(conditions.Find(a =>
                                                 a.Key == entity.ShardKeyName &&
                                                 a.Type == ConditionType.GreaterThanOrEqual).Value.ToString() ??
                                             throw new InvalidOperationException()) / int.Parse(entity.Step));
                        }

                        if (conditionType == ConditionType.LessThan)
                        {
                            var value = (int.Parse(
                                conditions.Find(a =>
                                        a.Key == entity.ShardKeyName && a.Type == ConditionType.LessThan)
                                    .Value.ToString() ??
                                throw new InvalidOperationException()) / int.Parse(entity.Step));
                            max = ((value - 1) % int.Parse(entity.Step) == 0)
                                ? Math.Min(max, value - 1)
                                : Math.Min(max, value);
                        }

                        if (conditionType == ConditionType.LessThanOrEqual)
                        {
                            max = Math.Min(max,
                                (int.Parse(conditions.Find(a =>
                                               a.Key == entity.ShardKeyName &&
                                               a.Type == ConditionType.LessThanOrEqual).Value.ToString() ??
                                           throw new InvalidOperationException()) / int.Parse(entity.Step)));
                        }
                    }
                }
            }
            if (findGroup) break;
        }

        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionName jump:  " +
                               $"conditions: {JsonConvert.SerializeObject(conditions)},min:{min},max:{max}");
        List<string> collectionNames = new List<string>();
        if (min > max)
        {
            return new List<string>() { };
        }

        for (long i = min; i <= max; i++)
        {
            var shardIndexName = (indexName + "-" + i).ToLower();
            ;
            var shardIndexNameExist = _existIndexShardDictionary.TryGetValue(
                _aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + shardIndexName, out var value);
            if (shardIndexNameExist)
            {
                collectionNames.Add(shardIndexName);
            }
            else
            {
                var client = _elasticsearchClientProvider.GetClient();
                var exits = await client.Indices.ExistsAsync(_aelfEntityMappingOptions.CollectionPrefix.ToLower() +
                                                             "." + shardIndexName);

                if (exits.Exists)
                {
                    _existIndexShardDictionary[
                        _aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + shardIndexName] = true;
                    collectionNames.Add(shardIndexName);
                }
            }
        }

        _logger.LogInformation(
            $"GetCollectionName: min: {min} , max: {max}, conditions: {JsonConvert.SerializeObject(conditions)}, indexName: {JsonConvert.SerializeObject(collectionNames)}");
        return collectionNames;
    }

    public async Task<string> GetCollectionName(TEntity entity)
    {
        var indexName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity)); 
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardingKeyByEntity();
        if (shardingKeyInfos.IsNullOrEmpty())
        {
            return indexName.ToLower();
        }
        
        foreach (var shardKeyInfo in shardingKeyInfos)
        {
            bool findGroup = false;
            foreach (var shardKey in shardKeyInfo.ShardKeys)
            {
                if (shardKey.StepType == StepType.None)
                {
                    //The field values of entity's sub table must be consistent with the configuration in the sub table configuration file
                    if (shardKey.Func(entity).ToString() == shardKey.Value)
                    {
                        indexName = indexName + "-" + shardKey.Func(entity) ??
                                    throw new InvalidOleVariantTypeException();
                        findGroup = true;
                    }
                }
                else
                {
                    if(!findGroup) continue;
                    if (shardKey.StepType != StepType.Floor)
                    {
                        throw new Exception(shardKey.ShardKeyName + "need config StepType equal Floor");
                    }

                    var value = shardKey.Func(entity);
                    if (shardKey.StepType != StepType.Floor)
                    {
                        throw new Exception(shardKey.ShardKeyName + "need config StepType equal Floor");
                    }

                    indexName = indexName + "-" +
                                int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardKey.Step);
                }
            }

            if (findGroup) break;
        }

        //addCache
        string[] collectionNameArr = indexName.ToLower().Split('-');
        var suffix = collectionNameArr.Last();
        var keys = indexName.ToLower().Substring(collectionNameArr[0].Length + 1,
            indexName.Length - suffix.Length - collectionNameArr[0].Length - 1);
        await AddCollectionMaxShardIndex(typeof(TEntity).Name, keys, long.Parse(suffix));
        return indexName.ToLower();
    }

    public async Task<List<string>> GetCollectionName(List<TEntity> entitys)
    {
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardingKeyByEntity();
        if (shardingKeyInfos.IsNullOrEmpty())
        {
            var collectionName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity)); 
            return new List<string>() { collectionName.ToLower() };
        }

        List<string> collectionNames = new List<string>();
        long maxShardNo = 0;
        string maxCollectionName = "";
      
        foreach (var entity in entitys)
        {
            var collectionName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity)); 
            string groupNo = "";
            foreach (var shardingKeyInfo in shardingKeyInfos)
            {
                bool findGroup = false;
                foreach (var shardInfo in shardingKeyInfo.ShardKeys)
                {
                    if (shardInfo.StepType == StepType.None)
                    {
                        if (shardInfo.Func(entity).ToString() == shardInfo.Value)
                        {
                            collectionName = collectionName + "-" + shardInfo.Func(entity) ??
                                             throw new InvalidOleVariantTypeException();
                            findGroup = true;
                        }
                    }
                    else
                    {
                        if(!findGroup) continue;
                        if (shardInfo.StepType != StepType.Floor)
                        {
                            throw new Exception(shardInfo.ShardKeyName + "need config StepType equal Floor");
                        }

                        var value = shardInfo.Func(entity);
                        collectionName = collectionName + "-" +
                                         int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardInfo.Step);
                        if (int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardInfo.Step) >= maxShardNo)
                        {
                            maxShardNo = int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardInfo.Step);
                            maxCollectionName = collectionName;
                        }
                    }
                }
                if (findGroup) break;
            }

            collectionNames.Add(collectionName.ToLower());
        }

        //addCache
        string[] collectionNameArr = maxCollectionName.ToLower().Split('-');
        var suffix = collectionNameArr.Last();
        var keys = maxCollectionName.ToLower().Substring(collectionNameArr[0].Length + 1,
            maxCollectionName.Length - suffix.Length - collectionNameArr[0].Length - 1);
        await AddCollectionMaxShardIndex(typeof(TEntity).Name, keys, long.Parse(suffix));
        return collectionNames;
    }

    public async Task AddOrUpdateAsync(ShardingCollectionTail model)
    {
        var indexName =
            (_aelfEntityMappingOptions.CollectionPrefix + "." + typeof(ShardingCollectionTail).Name).ToLower();
        await _elasticIndexService.CreateIndexAsync(indexName, typeof(ShardingCollectionTail),
            _indexSettingOptions.NumberOfShards, _indexSettingOptions.NumberOfReplicas);
        var client = _elasticsearchClientProvider.GetClient();
        var exits = client.DocumentExists(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName));

        if (exits.Exists)
        {
            var result = client.UpdateAsync(DocumentPath<ShardingCollectionTail>.Id(new Id(model)),
                ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_indexSettingOptions.Refresh));

            if (result.Result.IsValid) return;
            throw new Exception($"Update Document failed at index{indexName} :" +
                                result.Result.ServerError.Error.Reason);
        }
        else
        {
            var result = client.IndexAsync(model, ss => ss.Index(indexName).Refresh(_indexSettingOptions.Refresh));
            if (result.Result.IsValid) return;
            throw new Exception($"Insert Docuemnt failed at index {indexName} :" +
                                result.Result.ServerError.Error.Reason);
        }
    }

    public async Task<Tuple<long, List<ShardingCollectionTail>>> GetCollectionMaxShardIndexAsync(
        ShardingCollectionTail searchDto)
    {
        var indexName =
            (_aelfEntityMappingOptions.CollectionPrefix + "." + typeof(ShardingCollectionTail).Name).ToLower();
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionMaxShardIndex into create:  " +
                               $"searchDto: {JsonConvert.SerializeObject(searchDto)},indexName:{indexName}");
        await _elasticIndexService.CreateIndexAsync(indexName, typeof(ShardingCollectionTail),
            _indexSettingOptions.NumberOfShards, _indexSettingOptions.NumberOfReplicas);
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionMaxShardIndex out create:  " +
                               $"searchDto: {JsonConvert.SerializeObject(searchDto)},indexName:{indexName}");
        var client = _elasticsearchClientProvider.GetClient();
        var mustQuery = new List<Func<QueryContainerDescriptor<ShardingCollectionTail>, QueryContainer>>();
        mustQuery.Add(q => q.Term(i => i.Field(f => f.EntityName).Value(searchDto.EntityName)));
        mustQuery.Add(q => q.Term(i => i.Field(f => f.TailPrefix).Value(searchDto.TailPrefix)));
        QueryContainer Filter(QueryContainerDescriptor<ShardingCollectionTail> f) => f.Bool(b => b.Must(mustQuery));
        Func<SearchDescriptor<ShardingCollectionTail>, ISearchRequest> selector = null;
        Expression<Func<ShardingCollectionTail, object>> sortExp = k => k.Tail;
        selector = new Func<SearchDescriptor<ShardingCollectionTail>, ISearchRequest>(s =>
            s.Index(indexName).Query(Filter).Sort(st => st.Field(sortExp, SortOrder.Descending)));

        var result = await client.SearchAsync(selector);
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionMaxShardIndex:  " +
                               $"searchDto: {JsonConvert.SerializeObject(searchDto)},indexName:{indexName},result:{JsonConvert.SerializeObject(result)}");
        if (!result.IsValid)
        {
            throw new Exception($"Search document failed at index {indexName} :" + result.ServerError.Error.Reason);
        }

        return new Tuple<long, List<ShardingCollectionTail>>(result.Total, result.Documents.ToList());
    }

    private async Task AddCollectionMaxShardIndex(string entityName, string keys, long maxShardNo)
    {
        ShardingCollectionTail shardCollectionSuffix = new ShardingCollectionTail();
        shardCollectionSuffix.EntityName = entityName;
        shardCollectionSuffix.TailPrefix = keys;

        var result = await GetCollectionMaxShardIndexAsync(shardCollectionSuffix);

        List<ShardingCollectionTail> shardCollectionSuffixes = result.Item2;
        if (shardCollectionSuffixes.IsNullOrEmpty())
        {
            ShardingCollectionTail cacheDto = new ShardingCollectionTail();
            cacheDto.EntityName = entityName;
            cacheDto.TailPrefix = keys;
            cacheDto.Tail = maxShardNo;
            cacheDto.Id = Guid.NewGuid().ToString();
            await AddOrUpdateAsync(cacheDto);
            return;
        }

        ShardingCollectionTail shardCollectionCacheDto = shardCollectionSuffixes.Find(a => a.TailPrefix.Contains(keys));

        if (shardCollectionCacheDto != null && shardCollectionCacheDto.Tail < maxShardNo)
        {
            ShardingCollectionTail cacheDto = shardCollectionCacheDto;
            cacheDto.Tail = maxShardNo;
            await AddOrUpdateAsync(cacheDto);
            return;
        }
    }

    public bool IsShardingCollection()
    {
        List<ShardingKeyInfo<TEntity>> shardingKeyInfos = GetShardingKeyByEntity();
        if (shardingKeyInfos.IsNullOrEmpty())
        {
            return false;
        }

        return true;
    }

    private void InitShardProvider()
    {
        var type = typeof(TEntity);
        Type shardProviderType = typeof(IShardingKeyProvider<>).MakeGenericType(type);
        Type providerImplementationType = typeof(ShardingKeyProvider<>).MakeGenericType(type);

        object? providerObj = Activator.CreateInstance(providerImplementationType);

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        bool isShard = false;
        foreach (var property in properties)
        {
            ShardPropertyAttributes attribute =
                (ShardPropertyAttributes)Attribute.GetCustomAttribute(property, typeof(ShardPropertyAttributes));
            if (attribute != null)
            {
                var propertyExpression = GetPropertyExpression(type, property.Name);
                MethodInfo? method = shardProviderType.GetMethod("SetShardingKey");
                List<ShardGroup>? shardGroups =
                    _indexShardOptions.Find(a => a.CollectionName == type.Name)?.ShardGroups;
                if (shardGroups.IsNullOrEmpty())
                {
                    throw new Exception($"ShardGroup is null or empty,please check the config file");
                }
                for (int i = 0; i < shardGroups.Count; i++)
                {
                    ShardKey? shardKey = shardGroups[i]?.ShardKeys.Find(a => a.Name == property.Name);
                    SetShardingKey(i, shardGroups.Count(), property.Name, shardKey.Step, attribute.Order,
                        shardKey.Value, shardKey.StepType, propertyExpression.Body, propertyExpression.Parameters);
                }

                isShard = true;
            }
        }
        _isShardIndex = isShard ? InitShardType.IsShard : InitShardType.NotShard;
    }

    private LambdaExpression GetPropertyExpression(Type entityType, string propertyName)
    {
        var propertyInfo = entityType.GetProperty(propertyName);

        var parameter = Expression.Parameter(entityType, "entity");

        var propertyAccess = Expression.Property(parameter, propertyInfo);

        var lambda = Expression.Lambda(propertyAccess, parameter);

        return lambda;
    }
}

public enum InitShardType
{
    IsShard,
    NotShard
}