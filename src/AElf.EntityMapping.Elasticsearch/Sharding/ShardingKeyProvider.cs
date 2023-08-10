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
    private readonly List<ShardInitSettingDto> _indexShardOptions;
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ShardingKeyProvider<TEntity>> _logger;

    private int _isShardIndex = 0;//0-init ,1-yes,2-no
    public  List<ShardProviderEntity<TEntity>> ShardProviderEntityList = new List<ShardProviderEntity<TEntity>>();
    private readonly IDistributedCache<List<ShardCollectionCacheDto>> _indexCollectionCache;
    private Dictionary<string, string> _existIndexShardDictionary = new Dictionary<string, string>();

    public ShardingKeyProvider(IOptions<ElasticsearchOptions> indexSettingOptions, IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions, IDistributedCache<List<ShardCollectionCacheDto>> indexCollectionCache,IElasticIndexService elasticIndexService,IElasticsearchClientProvider elasticsearchClientProvider,
        ILogger<ShardingKeyProvider<TEntity>> logger)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _indexShardOptions = aelfEntityMappingOptions.Value.ShardInitSettings;
        _indexCollectionCache = indexCollectionCache;
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
        if (ShardProviderEntityList is null)
        {
            ShardProviderEntityList = new List<ShardProviderEntity<TEntity>>();
            ShardProviderEntityList.Add(new ShardProviderEntity<TEntity>(keyName, step.ToString(), order, value, groupNo, func));
        }else
        {
            ShardProviderEntityList.Add(new ShardProviderEntity<TEntity>(keyName,step.ToString(), order, value, groupNo, func));
        }
    }
    

    public ShardProviderEntity<TEntity> GetShardingKeyByEntityAndFieldName(TEntity entity, string fieldName)
    {
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(entity.GetType()).FindAll(a=>a.SharKeyName==fieldName);
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
        if ( _isShardIndex == 0 || ShardProviderEntityList is null || ShardProviderEntityList.Count == 0)
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

    private long GetShardCollectionMaxNo(List<CollectionNameCondition> conditions)
    {
        ShardCollectionSuffix shardCollectionSuffix = new ShardCollectionSuffix();
        shardCollectionSuffix.EntityName = typeof(TEntity).Name;
        var result = GetCollectionMaxShardIndex(shardCollectionSuffix);
        if (result is null || result.Result.Item1 == 0)
        {
            return 0;
        }

        List<ShardCollectionSuffix> catchList = result.Result.Item2;
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        if(entitys is null || entitys.Count == 0)
        {
            return 0;
        }
        foreach (var condition in conditions)
        {
            var entity = entitys.Find(a => a.SharKeyName == condition.Key && a.Step == "");
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

    private long GetShardCollectionCache(List<CollectionNameCondition> conditions)
    { 
        List<ShardCollectionCacheDto> list =  _indexCollectionCache.Get(typeof(TEntity).Name);
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        if(entitys is null || entitys.Count == 0)
        {
            return 0;
        }
        foreach (var condition in conditions)
        {
            var entity = entitys.Find(a => a.SharKeyName == condition.Key && a.Step == "");
            if (entity != null && list != null)
            {
                ShardCollectionCacheDto cacheDto =  list.Find(a => a.Keys == (condition.Value.ToString().ToLower() + "-"));
                if (cacheDto != null)
                {
                    return cacheDto.MaxShardNo;
                }
            }
        }
        return 0;
    }

    public List<string> GetCollectionName(List<CollectionNameCondition> conditions)
    {
        var indexName = _elasticIndexService.GetDefaultIndexName(typeof(TEntity)); 
        long min = 0;
       long max = GetShardCollectionMaxNo(conditions);
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
                if((groupNo == "" || entity.GroupNo == groupNo) && conditions.Find(a=>a.Key == entity.SharKeyName).Value.ToString() == entity.Value){ 
                    indexName = indexName + "-" + conditions.Find(a=>a.Key == entity.SharKeyName).Value ?? throw new InvalidOleVariantTypeException();
                    groupNo = groupNo == "" ? entity.GroupNo : groupNo;
                }
            }
            else
            {
                if (groupNo == "" || entity.GroupNo == groupNo)
                {
                   
                    var shardConditions = conditions.FindAll(a => a.Key == entity.SharKeyName);
                    foreach (var condition in shardConditions)
                    {
                        var conditionType = condition.Type;
                        if (conditionType == ConditionType.Equal)
                        {
                            indexName = indexName + "-" +
                                        (int.Parse(conditions.Find(a => a.Key == entity.SharKeyName).Value.ToString() ??
                                                   throw new InvalidOperationException()) / int.Parse(entity.Step));
                            groupNo = groupNo == "" ? entity.GroupNo : groupNo;

                            return new List<string>(){indexName.ToLower()};
                        }

                        if (conditionType == ConditionType.GreaterThan)
                        {
                            var value = (int.Parse(
                                conditions.Find(a => a.Key == entity.SharKeyName && a.Type == ConditionType.GreaterThan)
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
                            min =  (int.Parse(conditions.Find(a => a.Key == entity.SharKeyName && a.Type == ConditionType.GreaterThanOrEqual).Value.ToString() ??
                                              throw new InvalidOperationException()) / int.Parse(entity.Step));
                        }
                    
                        if (conditionType == ConditionType.LessThan)
                        {
                            var value = (int.Parse(
                                conditions.Find(a => a.Key == entity.SharKeyName && a.Type == ConditionType.LessThan)
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
                            max = Math.Min(max, (int.Parse(conditions.Find(a => a.Key == entity.SharKeyName && a.Type == ConditionType.LessThanOrEqual).Value.ToString() ??
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
                var exits = client.Indices.ExistsAsync(_aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + shardIndexName).Result;
            
                if (exits.Exists)
                {
                    _existIndexShardDictionary[_aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + shardIndexName] = "1";
                    collectionNames.Add(shardIndexName);
                }
            }
        }
        _logger.LogInformation($"GetCollectionName: min: {min} , max: {max}, conditions: {JsonConvert.SerializeObject(conditions)}, indexName: {JsonConvert.SerializeObject(collectionNames)}");
        return collectionNames;
    }

    public string GetCollectionName(TEntity entity)
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
        AddCollectionMaxShardIndex(typeof(TEntity).Name, indexName.ToLower());
        return indexName.ToLower();
    }

    public List<string> GetCollectionName(List<TEntity> entitys)
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
        AddCollectionMaxShardIndex(typeof(TEntity).Name, maxCollectionName.ToLower());
        return collectionNames;
    }
    
    
    private void SetShardCollectionCache(string entityName, string collectionName)
    {
        string[] split = collectionName.Split("-");
        string keys = "";
        long maxShardNo = 0;
        for(int i=0; i<split.Length; i++)
        {
            if (i == 0)
            {
                continue;
            }
            if(i == split.Length - 1)
            {
                maxShardNo = long.Parse(split[i]);
                break;
            }
            keys = keys + split[i] + "-";
        }
        List<ShardCollectionCacheDto> shardCollectionCacheDtos = _indexCollectionCache.Get(typeof(TEntity).Name);
        if (shardCollectionCacheDtos is null)
        {
            shardCollectionCacheDtos = new List<ShardCollectionCacheDto>();
            ShardCollectionCacheDto cacheDto = new ShardCollectionCacheDto();
            
            cacheDto.Keys = keys;
            cacheDto.MaxShardNo = maxShardNo;
            shardCollectionCacheDtos.Add(cacheDto);
            _indexCollectionCache.Set(entityName, shardCollectionCacheDtos);
            return;
        }
        
        ShardCollectionCacheDto shardCollectionCacheDto = shardCollectionCacheDtos.Find(a => a.Keys == keys);
        if (shardCollectionCacheDto is null)
        {
            ShardCollectionCacheDto cacheDto = new ShardCollectionCacheDto();
            cacheDto.Keys = keys;
            cacheDto.MaxShardNo = maxShardNo;
            shardCollectionCacheDtos.Add(cacheDto);
            _indexCollectionCache.Set(entityName, shardCollectionCacheDtos);
            return;
        }
        else
        {
            long oldMaxShardNo = shardCollectionCacheDto.MaxShardNo;
            if(oldMaxShardNo < maxShardNo)
            {
                shardCollectionCacheDtos.Remove(shardCollectionCacheDto);
                ShardCollectionCacheDto currentCacheDto = new ShardCollectionCacheDto();
                currentCacheDto.MaxShardNo = maxShardNo;
                currentCacheDto.Keys = keys;
                shardCollectionCacheDtos.Add(currentCacheDto);
            }
            _indexCollectionCache.Set(entityName, shardCollectionCacheDtos);
            return;
        }
    }

    public async Task AddOrUpdateAsync(ShardCollectionSuffix model)
    {
        var indexName = (_aelfEntityMappingOptions.CollectionPrefix + "." + typeof(ShardCollectionSuffix).Name).ToLower();
        await CreateShardCollectionSuffixIndex(indexName);
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
    public async Task<Tuple<long, List<ShardCollectionSuffix>>> GetCollectionMaxShardIndex(ShardCollectionSuffix searchDto)
    {
        var indexName = (_aelfEntityMappingOptions.CollectionPrefix + "." + typeof(ShardCollectionSuffix).Name).ToLower();
        _logger.LogInformation($"ElasticsearchCollectionNameProvider.GetCollectionMaxShardIndex:  " +
                               $"searchDto: {JsonConvert.SerializeObject(searchDto)},indexName:{indexName}");
        CreateShardCollectionSuffixIndex(indexName);
        var client = _elasticsearchClientProvider.GetClient();
        var mustQuery = new List<Func<QueryContainerDescriptor<ShardCollectionSuffix>, QueryContainer>>();
        mustQuery.Add(q => q.Term(i => i.Field(f => f.EntityName).Value(searchDto.EntityName)));
        mustQuery.Add(q => q.Term(i => i.Field(f => f.Keys).Value(searchDto.Keys)));
        QueryContainer Filter(QueryContainerDescriptor<ShardCollectionSuffix> f) => f.Bool(b => b.Must(mustQuery));
        Func<SearchDescriptor<ShardCollectionSuffix>, ISearchRequest> selector = null;
        Expression<Func<ShardCollectionSuffix, object>> sortExp = k => k.MaxShardNo;
        selector = new Func<SearchDescriptor<ShardCollectionSuffix>, ISearchRequest>(s => s.Index(indexName).Query(Filter).Sort(st=>st.Field(sortExp,SortOrder.Descending)).From(0).Size(1));
        
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
        
        var result = GetCollectionMaxShardIndex(shardCollectionSuffix);
        
        List<ShardCollectionSuffix> shardCollectionCacheDtos = result.Result.Item2;
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
        
        if(shardCollectionCacheDtos.Last().MaxShardNo < long.Parse(suffix))
        {
            ShardCollectionSuffix cacheDto = shardCollectionCacheDtos.Last();
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
                if((groupNo == "" || entity.GroupNo == groupNo) && conditions[entity.SharKeyName] == entity.Value){ 
                    indexName = indexName + "-" + conditions[entity.SharKeyName] ?? throw new InvalidOleVariantTypeException();
                    groupNo = groupNo == "" ? entity.GroupNo : groupNo;
                }
            }
            else
            {
                if (groupNo == "" || entity.GroupNo == groupNo)
                {
                    indexName = indexName + "-" +
                                (int.Parse(conditions[entity.SharKeyName].ToString() ??
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
        _isShardIndex = isShard ? 1 : 2;
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


