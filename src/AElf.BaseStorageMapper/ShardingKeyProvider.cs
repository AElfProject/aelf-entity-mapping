using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using AElf.BaseStorageMapper.Entities;
using AElf.BaseStorageMapper.Options;
using AElf.BaseStorageMapper.Sharding;
using Microsoft.Extensions.Options;
using Volo.Abp.Caching;

namespace AElf.BaseStorageMapper;

public class ShardingKeyProvider<TEntity> : IShardingKeyProvider<TEntity> where TEntity : class
{
    private readonly IndexSettingOptions _indexSettingOptions;
    private readonly ShardInitSettingOptions _indexShardOptions;
    private int _isShardIndex = 0;//0-init ,1-yes,2-no
    public  List<ShardProviderEntity<TEntity>> ShardProviderEntityList = new List<ShardProviderEntity<TEntity>>();
    private readonly IDistributedCache<List<ShardCollectionCacheDto>> _indexCollectionCache;

    public ShardingKeyProvider(IOptions<IndexSettingOptions> indexSettingOptions, IOptions<ShardInitSettingOptions> indexShardOptions, IDistributedCache<List<ShardCollectionCacheDto>> indexCollectionCache)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _indexShardOptions = indexShardOptions.Value;
        _indexCollectionCache = indexCollectionCache;

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
        //return GetShardingKeyByEntity(entity).Find(a=>a.SharKeyName==fieldName);
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
        //return _getPropertyFunc.FindAll(a=> a.Func(entity) != null);
    }

    public List<string> GetCollectionName(List<CollectionNameCondition> conditions)
    {
        var indexName = _indexSettingOptions.IndexPrefix + "." + typeof(TEntity).Name;
        int min = 0;
        int max = 0;
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
                if((groupNo == "" || entity.GroupNo == groupNo) && conditions.Find(a=>a.Key == entity.SharKeyName).Value == entity.Value){ 
                    indexName = indexName + "-" + conditions.Find(a=>a.Key == entity.SharKeyName).Value ?? throw new InvalidOleVariantTypeException();
                    groupNo = groupNo == "" ? entity.GroupNo : groupNo;
                }
            }
            else
            {
                if (groupNo == "" || entity.GroupNo == groupNo)
                {
                    var conditionType = conditions.Find(a => a.Key == entity.SharKeyName).Type;
                    if (conditionType == ConditionType.Equal)
                    {
                        indexName = indexName + "-" +
                                    (int.Parse(conditions.Find(a => a.Key == entity.SharKeyName).Value.ToString() ??
                                               throw new InvalidOperationException()) / int.Parse(entity.Step));
                        groupNo = groupNo == "" ? entity.GroupNo : groupNo;
                    }

                    if (conditionType == ConditionType.GreaterThan)
                    {
                        
                    }
                    
                    if (conditionType == ConditionType.GreaterThanOrEqual)
                    {
                        
                    }
                    
                    if (conditionType == ConditionType.LessThan)
                    {
                        
                    }
                    
                    if (conditionType == ConditionType.LessThanOrEqual)
                    {
                        
                    }
                }
            }
        }

        return new List<string>(){indexName.ToLower()};
        /*List<ShardCollectionCacheDto> list =  _indexCollectionCache.Get(typeof(TEntity).Name);
        return null;*/
    }

    public string GetCollectionName(TEntity entity)
    {
        var indexName = _indexSettingOptions.IndexPrefix + "." + typeof(TEntity).Name;
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
        var indexName = _indexSettingOptions.IndexPrefix + "." + typeof(TEntity).Name;
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
       // Type type = entity.GetType();
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
                List<ShardChain>? shardChains = _indexShardOptions.ShardInitSettings.Find(a => a.IndexName == type.Name)?.ShardChains;
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

public class ShardProviderEntity<TEntity> where TEntity : class
{
    public string SharKeyName { get; set; }
    public string Step { get; set; }
    
    public int Order { get; set; }
    
    public string Value { get; set; }
    
    public string GroupNo { get; set; }
    public Func<TEntity, object> Func { get; set; }
    
    public ShardProviderEntity(string keyName, string step, int order, string value, string groupNo, Func<TEntity, object> func)
    {
        SharKeyName = keyName;
        Func = func;
        Step = step;
        Order = order;
        Value = value;
        GroupNo = groupNo;
    }

}
public class ShardProviderEntityComparer<TEntity> : IComparer<ShardProviderEntity<TEntity>> where TEntity : class
{
    public int Compare(ShardProviderEntity<TEntity> x, ShardProviderEntity<TEntity> y)
    {
        if (x == null && y == null)
        {
            return 0;
        }
        else if (x == null)
        {
            return -1;
        }
        else if (y == null)
        {
            return 1;
        }
        else
        {
            return x.Order.CompareTo(y.Order);
        }
    }
}