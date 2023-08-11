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
using Volo.Abp.Caching;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class ShardingKeyProvider<TEntity> : IShardingKeyProvider<TEntity> where TEntity : class
{
    private readonly ElasticsearchOptions _indexSettingOptions;
    private readonly IElasticIndexService _elasticIndexService;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly List<ShardInitSetting> _indexShardOptions;
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ShardingKeyProvider<TEntity>> _logger;

    private InitShardStatus _isShardIndex = InitShardStatus.None;
    public  List<ShardProviderEntity<TEntity>> ShardProviderEntityList = new List<ShardProviderEntity<TEntity>>();
    private Dictionary<string, bool> _existIndexShardDictionary = new Dictionary<string, bool>();

    public ShardingKeyProvider(IOptions<ElasticsearchOptions> indexSettingOptions, IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions, IDistributedCache<List<ShardCollectionCacheDto>> indexCollectionCache,IElasticIndexService elasticIndexService,IElasticsearchClientProvider elasticsearchClientProvider,
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

    public void SetShardingKey(string keyName, string step, int order, string value, string groupNo, Expression body, ReadOnlyCollection<ParameterExpression> parameterExpressions)
    {
        var expression = Expression.Lambda<Func<TEntity, object>>(
            Expression.Convert(body, typeof(object)), parameterExpressions);
        var func = expression.Compile();
        ShardProviderEntityList.Add(new ShardProviderEntity<TEntity>(keyName,step.ToString(), order, value, groupNo, func));
    }
    

    public ShardProviderEntity<TEntity> GetShardingKeyByEntityAndFieldName(TEntity entity, string fieldName)
    {
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(entity.GetType()).FindAll(a=>a.ShardKeyName==fieldName);
        foreach (var shardProviderEntity in entitys)
        {
            if (shardProviderEntity.Value != null && shardProviderEntity.Value != "" && shardProviderEntity.Value != "0")
            {
                return entitys.Find(a => a.Value == a.Func(entity));
            }
        }

        return (entitys == null || entitys.Count == 0) ? null : entitys.First();
    }
    
    public List<ShardProviderEntity<TEntity>> GetShardingKeyByEntity(Type type)
    {
        if ( _isShardIndex == InitShardStatus.None || ShardProviderEntityList is null || ShardProviderEntityList.Count == 0)
        {
            if (CheckCollectionType(type))
            {
                InitShardProvider(type);
            }
            else
            {
                return null!;
            }
        }

        return ShardProviderEntityList;
    }

    private async Task<long> GetShardCollectionMaxNoAsync(List<CollectionNameCondition> conditions)
    {
        ShardCollectionSuffix shardCollectionSuffix = new ShardCollectionSuffix();
        shardCollectionSuffix.EntityName = typeof(TEntity).Name;
        var result = await GetCollectionMaxShardIndexAsync(shardCollectionSuffix);
        if (result is null || result.Item1 == 0)
        {
            return 0;
        }

        List<ShardCollectionSuffix> catchList = result.Item2;
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        if(entitys is null || entitys.Count == 0)
        {
            return 0;
        }
        foreach (var condition in conditions)
        {
            var entity = entitys.Find(a => a.ShardKeyName == condition.Key && a.Step == "");
            if (entity != null && catchList != null)
            {
                ShardCollectionSuffix cacheDto =  catchList.Find(a => a.Keys.StartsWith(condition.Value.ToString().ToLower()));
                if (cacheDto != null)
                {
                    return cacheDto.MaxShardNo;
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
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        
        if (entitys is null || entitys.Count == 0)
        {
            return new List<string>(){indexName.ToLower()};
        }
        
        string groupNo = "";
        foreach (var entity in entitys)
        {
            if (entity.Step == "")
            {
                if((groupNo == "" || entity.GroupNo == groupNo) && conditions.Find(a=>a.Key == entity.ShardKeyName).Value.ToString() == entity.Value){ 
                    indexName = indexName + "-" + conditions.Find(a=>a.Key == entity.ShardKeyName).Value ?? throw new InvalidOleVariantTypeException();
                    groupNo = groupNo == "" ? entity.GroupNo : groupNo;
                }
            }
            else
            {
                if (groupNo == "" || entity.GroupNo == groupNo)
                {
                   
                    var shardConditions = conditions.FindAll(a => a.Key == entity.ShardKeyName);
                    foreach (var condition in shardConditions)
                    {
                        var conditionType = condition.Type;
                        if (conditionType == ConditionType.Equal)
                        {
                            indexName = indexName + "-" +
                                        (int.Parse(conditions.Find(a => a.Key == entity.ShardKeyName).Value.ToString() ??
                                                   throw new InvalidOperationException()) / int.Parse(entity.Step));
                            groupNo = groupNo == "" ? entity.GroupNo : groupNo;

                            return new List<string>(){indexName.ToLower()};
                        }

                        if (conditionType == ConditionType.GreaterThan)
                        {
                            var value = (int.Parse(
                                conditions.Find(a => a.Key == entity.ShardKeyName && a.Type == ConditionType.GreaterThan)
                                    .Value.ToString() ??
                                throw new InvalidOperationException()) / int.Parse(entity.Step));
                            if((value+1)%int.Parse(entity.Step) == 0)
                            {
                                min = value + 1;
                            }
                            else
                            {
                                min = value;
                            }
                        }
                    
                        if (conditionType == ConditionType.GreaterThanOrEqual)
                        {
                            min =  (int.Parse(conditions.Find(a => a.Key == entity.ShardKeyName && a.Type == ConditionType.GreaterThanOrEqual).Value.ToString() ??
                                              throw new InvalidOperationException()) / int.Parse(entity.Step));
                        }
                    
                        if (conditionType == ConditionType.LessThan)
                        {
                            var value = (int.Parse(
                                conditions.Find(a => a.Key == entity.ShardKeyName && a.Type == ConditionType.LessThan)
                                    .Value.ToString() ??
                                throw new InvalidOperationException()) / int.Parse(entity.Step));
                            if((value-1) % int.Parse(entity.Step) == 0)
                            {
                                max = Math.Min(max, value-1);
                            }
                            else
                            {
                                max = Math.Min(max, value);
                            }
                        }
                    
                        if (conditionType == ConditionType.LessThanOrEqual)
                        {
                            max = Math.Min(max, (int.Parse(conditions.Find(a => a.Key == entity.ShardKeyName && a.Type == ConditionType.LessThanOrEqual).Value.ToString() ??
                                                           throw new InvalidOperationException()) / int.Parse(entity.Step)));
                        }
                    }
                }
            }
        }
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionName jump:  " +
                               $"conditions: {JsonConvert.SerializeObject(conditions)},min:{min},max:{max}");
        List<string> collectionNames = new List<string>();
        if (min > max)
        {
            //return new List<string>(){(indexName + "-" + min).ToLower()};
            return new List<string>(){};
        }
        
        for(long i = min; i <= max; i++)
        {
            var shardIndexName = (indexName + "-" + i).ToLower();
            ;
            var shardIndexNameExist = _existIndexShardDictionary.TryGetValue(_aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + shardIndexName, out var value);
            if (shardIndexNameExist)
            {
                collectionNames.Add(shardIndexName);
            }
            else
            {
                var client = _elasticsearchClientProvider.GetClient();
                var exits = await client.Indices.ExistsAsync(_aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + shardIndexName);
            
                if (exits.Exists)
                {
                    _existIndexShardDictionary[_aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + shardIndexName] = true;
                    collectionNames.Add(shardIndexName);
                }
            }
        }
        _logger.LogInformation($"GetCollectionName: min: {min} , max: {max}, conditions: {JsonConvert.SerializeObject(conditions)}, indexName: {JsonConvert.SerializeObject(collectionNames)}");
        return collectionNames;
    }

    public async Task<string> GetCollectionName(TEntity entity)
    {
        var indexName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity));
        List<ShardProviderEntity<TEntity>> sahrdEntitys = GetShardingKeyByEntity(typeof(TEntity));
        if (sahrdEntitys is null || sahrdEntitys.Count == 0)
        {
            return indexName.ToLower();
        }
        string groupNo = "";
        foreach (var shardEntity in sahrdEntitys)
        {
            if (shardEntity.Step == "")
            {
                if ((groupNo == "" || shardEntity.GroupNo == groupNo) && shardEntity.Func(entity).ToString() == shardEntity.Value)
                {
                    indexName = indexName + "-" + shardEntity.Func(entity) ?? throw new InvalidOleVariantTypeException();
                    groupNo = groupNo == "" ? shardEntity.GroupNo : groupNo;
                }
            }
            else
            {
                if (groupNo == "" || shardEntity.GroupNo == groupNo)
                {
                    var value = shardEntity.Func(entity);
                    indexName = indexName + "-" + int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardEntity.Step);
                    groupNo = groupNo == "" ? shardEntity.GroupNo : groupNo;
                }
            }
        }
        //addCache
        await AddCollectionMaxShardIndex(typeof(TEntity).Name, indexName.ToLower());
        return indexName.ToLower();
    }

    public async Task<List<string>> GetCollectionName(List<TEntity> entitys)
    {
        List<ShardProviderEntity<TEntity>> sahrdEntitys = GetShardingKeyByEntity(typeof(TEntity));
        if (sahrdEntitys is null || sahrdEntitys.Count == 0)
        {
            var collectionName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity));
            return new List<string>(){collectionName.ToLower()};
        }

        List<string> collectionNames = new List<string>();
        long maxShardNo = 0;
        string maxCollectionName = "";
        foreach (var entity in entitys)
        {
            var collectionName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity));
            string groupNo = "";
            foreach (var shardEntity in sahrdEntitys)
            {
                if (shardEntity.Step == "")
                {
                    if ((groupNo == "" || shardEntity.GroupNo == groupNo) && shardEntity.Func(entity).ToString() == shardEntity.Value)
                    {
                        collectionName = collectionName + "-" + shardEntity.Func(entity) ?? throw new InvalidOleVariantTypeException();
                        groupNo = groupNo == "" ? shardEntity.GroupNo : groupNo;
                    }
                }
                else
                {
                    if (groupNo == "" || shardEntity.GroupNo == groupNo)
                    {
                        var value = shardEntity.Func(entity);
                        collectionName = collectionName + "-" + int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardEntity.Step);
                        groupNo = groupNo == "" ? shardEntity.GroupNo : groupNo;
                        if (int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardEntity.Step) >= maxShardNo)
                        {
                            maxShardNo = int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardEntity.Step);
                            maxCollectionName = collectionName;
                        }
                    }
                }
            }
            collectionNames.Add(collectionName.ToLower());
        }
        //addCache
        await AddCollectionMaxShardIndex(typeof(TEntity).Name, maxCollectionName.ToLower());
        return collectionNames;
    }

    public async Task AddOrUpdateAsync(ShardCollectionSuffix model)
    {
        var indexName = (_aelfEntityMappingOptions.CollectionPrefix + "." + typeof(ShardCollectionSuffix).Name).ToLower();
        await _elasticIndexService.CreateIndexAsync(indexName, typeof(ShardCollectionSuffix), _indexSettingOptions.NumberOfShards, _indexSettingOptions.NumberOfReplicas);
        var client = _elasticsearchClientProvider.GetClient();
        var exits = client.DocumentExists(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName));

        if (exits.Exists)
        {
            var result = client.UpdateAsync(DocumentPath<ShardCollectionSuffix>.Id(new Id(model)),
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
    public async Task<Tuple<long, List<ShardCollectionSuffix>>> GetCollectionMaxShardIndexAsync(ShardCollectionSuffix searchDto)
    {
        var indexName = (_aelfEntityMappingOptions.CollectionPrefix + "." + typeof(ShardCollectionSuffix).Name).ToLower();
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionMaxShardIndex into create:  " +
                               $"searchDto: {JsonConvert.SerializeObject(searchDto)},indexName:{indexName}");
        await _elasticIndexService.CreateIndexAsync(indexName, typeof(ShardCollectionSuffix), _indexSettingOptions.NumberOfShards, _indexSettingOptions.NumberOfReplicas);
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionMaxShardIndex out create:  " +
                               $"searchDto: {JsonConvert.SerializeObject(searchDto)},indexName:{indexName}");
        var client = _elasticsearchClientProvider.GetClient();
        var mustQuery = new List<Func<QueryContainerDescriptor<ShardCollectionSuffix>, QueryContainer>>();
        mustQuery.Add(q => q.Term(i => i.Field(f => f.EntityName).Value(searchDto.EntityName)));
        mustQuery.Add(q => q.Term(i => i.Field(f => f.Keys).Value(searchDto.Keys)));
        QueryContainer Filter(QueryContainerDescriptor<ShardCollectionSuffix> f) => f.Bool(b => b.Must(mustQuery));
        Func<SearchDescriptor<ShardCollectionSuffix>, ISearchRequest> selector = null;
        Expression<Func<ShardCollectionSuffix, object>> sortExp = k => k.MaxShardNo;
        selector = new Func<SearchDescriptor<ShardCollectionSuffix>, ISearchRequest>(s => s.Index(indexName).Query(Filter).Sort(st=>st.Field(sortExp,SortOrder.Descending)));
        
        var result = await client.SearchAsync(selector);
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionMaxShardIndex:  " +
                               $"searchDto: {JsonConvert.SerializeObject(searchDto)},indexName:{indexName},result:{JsonConvert.SerializeObject(result)}");
        if (!result.IsValid)
        {
            throw new Exception($"Search document failed at index {indexName} :" + result.ServerError.Error.Reason);
        }
        return new Tuple<long, List<ShardCollectionSuffix>>(result.Total, result.Documents.ToList());
    }
    
    private async Task CreateShardCollectionSuffixIndex(string indexName)
    {
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.CreateShardCollectionSuffixIndex into:  " +
                               $"indexName:{indexName}");
        var client = _elasticsearchClientProvider.GetClient();
        var exits = await client.Indices.ExistsAsync(indexName);
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.CreateShardCollectionSuffixIndex:  " +
                               $"indexName:{indexName},exits:{exits.Exists}");
        if (exits.Exists)
        {
            return;
        }
        _logger.LogInformation($"create index , index name: {indexName}");
        var result = await client
            .Indices.CreateAsync(indexName,
                ss =>
                    ss.Index(indexName)
                        .Settings(
                            o => o.NumberOfShards(_indexSettingOptions.NumberOfShards).NumberOfReplicas(_indexSettingOptions.NumberOfReplicas)
                                .Setting("max_result_window", int.MaxValue))
                        .Map(m => m.AutoMap(typeof(ShardCollectionSuffix))));
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.CreateShardCollectionSuffixIndex:  " +
                               $"indexName:{indexName},result:{result.Acknowledged}");
        if (!result.Acknowledged)
            throw new Exception($"Create Index {indexName} failed : " +
                                             result.ServerError.Error.Reason);
    }
    
    private async Task AddCollectionMaxShardIndex(string entityName, string collectionName)
    {
        string[] collectionNameArr = collectionName.Split('-');
        var suffix = collectionNameArr.Last();
        var keys = collectionName.Substring(collectionNameArr[0].Length+1, collectionName.Length - suffix.Length - collectionNameArr[0].Length-1);
        
        ShardCollectionSuffix shardCollectionSuffix = new ShardCollectionSuffix();
        shardCollectionSuffix.EntityName = entityName;
        shardCollectionSuffix.Keys = keys;
        
        var result = await GetCollectionMaxShardIndexAsync(shardCollectionSuffix);
        
        List<ShardCollectionSuffix> shardCollectionCacheDtos = result.Item2;
        if (shardCollectionCacheDtos.IsNullOrEmpty())
        {
            ShardCollectionSuffix cacheDto = new ShardCollectionSuffix();
            cacheDto.EntityName = entityName;
            cacheDto.Keys = keys;
            cacheDto.MaxShardNo = long.Parse(suffix);
            cacheDto.Id = Guid.NewGuid().ToString();
            await AddOrUpdateAsync(cacheDto);
            return;
        }

        ShardCollectionSuffix shardCollectionCacheDto = shardCollectionCacheDtos.Find(a => a.Keys.Contains(keys));
        
        if(shardCollectionCacheDto != null && shardCollectionCacheDto.MaxShardNo < long.Parse(suffix))
        {
            ShardCollectionSuffix cacheDto = shardCollectionCacheDto;
            cacheDto.MaxShardNo = long.Parse(suffix);
            await AddOrUpdateAsync(cacheDto);
            return;
        }
    }

    public bool IsShardingCollection()
    {
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        if (entitys is null || entitys.Count == 0)
        {
            return false;
        }

        return true;
    }

    public string GetCollectionName(Dictionary<string, object> conditions)
    {
        var indexName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity));
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        if (entitys is null || entitys.Count == 0)
        {
            return indexName.ToLower();
        }
        
        string groupNo = "";
        foreach (var entity in entitys)
        {
            if (entity.Step == "")
            {
                if((groupNo == "" || entity.GroupNo == groupNo) && conditions[entity.ShardKeyName] == entity.Value){ 
                    indexName = indexName + "-" + conditions[entity.ShardKeyName] ?? throw new InvalidOleVariantTypeException();
                    groupNo = groupNo == "" ? entity.GroupNo : groupNo;
                }
            }
            else
            {
                if (groupNo == "" || entity.GroupNo == groupNo)
                {
                    indexName = indexName + "-" +
                                (int.Parse(conditions[entity.ShardKeyName].ToString() ??
                                           throw new InvalidOperationException()) / int.Parse(entity.Step));
                    groupNo = groupNo == "" ? entity.GroupNo : groupNo;
                }
            }
        }

        return indexName.ToLower();
    }

    
    public void InitShardProvider(Type type)
    {
        Type shardProviderType = typeof(IShardingKeyProvider<>).MakeGenericType(type);
        Type providerImplementationType = typeof(ShardingKeyProvider<>).MakeGenericType(type);
        
        object? providerObj = Activator.CreateInstance(providerImplementationType);

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        bool isShard = false;
        foreach (var property in properties)
        {
            ShardPropertyAttributes attribute = (ShardPropertyAttributes)Attribute.GetCustomAttribute(property, typeof(ShardPropertyAttributes));
            if(attribute != null)
            {
                var propertyExpression = GetPropertyExpression(type, property.Name);
                MethodInfo? method = shardProviderType.GetMethod("SetShardingKey");
                List<ShardGroup>? shardGroups = _indexShardOptions.Find(a => a.IndexName == type.Name)?.ShardGroups;
                foreach (var shardGroup in shardGroups)
                {
                    ShardKey? shardKey = shardGroup?.ShardKeys.Find(a => a.Name == property.Name);
                    method?.Invoke(providerObj, new object[] {property.Name, shardKey.Step, attribute.Order, shardKey.Value, shardKey.GroupNo, propertyExpression.Body, propertyExpression.Parameters}); 
                }

                isShard = true;
            }
        }
        object? getPropertyFunc = providerObj.GetType().GetField("ShardProviderEntityList").GetValue(providerObj);
        ShardProviderEntityList = (List<ShardProviderEntity<TEntity>>)getPropertyFunc;
        ShardProviderEntityList.Sort(new ShardProviderEntityComparer<TEntity>());
        _isShardIndex = isShard ? InitShardStatus.IsShard : InitShardStatus.NotShard;
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
public enum InitShardStatus
{
    None,
    IsShard,
    NotShard
}


