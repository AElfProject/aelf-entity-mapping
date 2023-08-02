using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Options;
using Volo.Abp.Caching;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class ShardingKeyProvider<TEntity> : IShardingKeyProvider<TEntity> where TEntity : class
{
    private readonly ElasticsearchOptions _indexSettingOptions;
    private readonly IElasticIndexService _elasticIndexService;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly List<ShardInitSettingDto> _indexShardOptions;

    private int _isShardIndex = 0;//0-init ,1-yes,2-no
    public  List<ShardProviderEntity<TEntity>> ShardProviderEntityList = new List<ShardProviderEntity<TEntity>>();
    private readonly IDistributedCache<List<ShardCollectionCacheDto>> _indexCollectionCache;

    public ShardingKeyProvider(IOptions<ElasticsearchOptions> indexSettingOptions, IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions, IDistributedCache<List<ShardCollectionCacheDto>> indexCollectionCache,IElasticIndexService elasticIndexService)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _indexShardOptions = aelfEntityMappingOptions.Value.ShardInitSettings;
        _indexCollectionCache = indexCollectionCache;
        _elasticIndexService = elasticIndexService;
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
            if (entity != null)
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
        var indexName = _aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + typeof(TEntity).Name.ToLower();
        long min = 0;
        long max = GetShardCollectionCache(conditions);
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        if (entitys is null || entitys.Count == 0)
        {
            return new List<string>(){indexName.ToLower()};
        }

        List<ShardCollectionCacheDto> list = _indexCollectionCache.Get(typeof(TEntity).Name);
        
        string groupNo = "";
        foreach (var entity in entitys)
        {
            if (entity.Step == "")
            {
                if((groupNo == "" || entity.GroupNo == groupNo) && conditions.Find(a=>a.Key == entity.SharKeyName).Value == entity.Value){ 
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
                            min = (int.Parse(conditions.Find(a => a.Key == entity.SharKeyName).Value.ToString() ??
                                             throw new InvalidOperationException()) / int.Parse(entity.Step)) + 1;
                        }
                    
                        if (conditionType == ConditionType.GreaterThanOrEqual)
                        {
                            min =  (int.Parse(conditions.Find(a => a.Key == entity.SharKeyName).Value.ToString() ??
                                              throw new InvalidOperationException()) / int.Parse(entity.Step));
                        }
                    
                        if (conditionType == ConditionType.LessThan)
                        {
                            max = Math.Min(max, (int.Parse(conditions.Find(a => a.Key == entity.SharKeyName).Value.ToString() ??
                                                           throw new InvalidOperationException()) / int.Parse(entity.Step)) - 1);
                        }
                    
                        if (conditionType == ConditionType.LessThanOrEqual)
                        {
                            max = Math.Min(max, (int.Parse(conditions.Find(a => a.Key == entity.SharKeyName).Value.ToString() ??
                                                           throw new InvalidOperationException()) / int.Parse(entity.Step)));
                        }
                    }
                }
            }
        }

        List<string> collectionNames = new List<string>();
        for(long i = min; i <= max; i++)
        {
            collectionNames.Add((indexName + "-" + i).ToLower());
        }

        return collectionNames;
    }

    public string GetCollectionName(TEntity entity)
    {
        var indexName = _aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + typeof(TEntity).Name.ToLower();
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
        SetShardCollectionCache(typeof(TEntity).Name, indexName.ToLower());
        return indexName.ToLower();
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
        var indexName = _aelfEntityMappingOptions.CollectionPrefix.ToLower() + "." + typeof(TEntity).Name.ToLower();
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
                List<ShardChain>? shardChains = _indexShardOptions.Find(a => a.IndexName == type.Name)?.ShardChains;
                foreach (var shardChain in shardChains)
                {
                    ShardKey? shardKey = shardChain?.ShardKeys.Find(a => a.Name == property.Name);
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


