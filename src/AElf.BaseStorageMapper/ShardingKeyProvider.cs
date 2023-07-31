using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using AElf.BaseStorageMapper.Options;
using Microsoft.Extensions.Options;

namespace AElf.BaseStorageMapper;

public class ShardingKeyProvider<TEntity> : IShardingKeyProvider<TEntity> where TEntity : class
{
    private readonly IndexSettingOptions _indexSettingOptions;
    private readonly ShardInitSettingOptions _indexShardOptions;
    private int _isShardIndex = 0;//0-init ,1-yes,2-no
    public  List<ShardProviderEntity<TEntity>> _getPropertyFunc = new List<ShardProviderEntity<TEntity>>();
    
    public ShardingKeyProvider(IOptions<IndexSettingOptions> indexSettingOptions, IOptions<ShardInitSettingOptions> indexShardOptions)
    {
        _indexSettingOptions = indexSettingOptions.Value;
        _indexShardOptions = indexShardOptions.Value;

    }
    public ShardingKeyProvider()
    {
    }
    
    private bool CheckCollectionType(Type type)
    {
        var compareType = typeof(IIndexBuild);
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
        if (_getPropertyFunc is null)
        {
            _getPropertyFunc = new List<ShardProviderEntity<TEntity>>();
            _getPropertyFunc.Add(new ShardProviderEntity<TEntity>(keyName, step.ToString(), order, value, groupNo, func));
        }else
        {
            _getPropertyFunc.Add(new ShardProviderEntity<TEntity>(keyName,step.ToString(), order, value, groupNo, func));
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
        if ( _isShardIndex == 0 || _getPropertyFunc is null || _getPropertyFunc.Count == 0)
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

        return _getPropertyFunc;
        //return _getPropertyFunc.FindAll(a=> a.Func(entity) != null);
    }

    public string GetCollectionName(Dictionary<string, object> conditions)
    {
        var indexName = _indexSettingOptions.IndexPrefix + "." + typeof(TEntity).Name;
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        if (entitys is null || entitys.Count == 0)
        {
            return indexName;
        }

        /*List<ShardProviderEntity<TEntity>> filterEntitys = new List<ShardProviderEntity<TEntity>>();
        foreach (var dictionary in conditions)
        {
            filterEntitys.Add(entitys.Find(a => a.SharKeyName == dictionary.Key));
        }*/
        entitys.Sort(new ShardProviderEntityComparer<TEntity>());

        foreach (var entity in entitys)
        {
            if (entity.Step == "")
            {
                indexName = "-" + conditions[entity.SharKeyName] ?? throw new InvalidOleVariantTypeException();
            }
            else
            {
                indexName = "-" + (int.Parse(conditions[entity.SharKeyName].ToString() ?? throw new InvalidOperationException()) / int.Parse(entity.Step));
            }
        }

        return indexName;
    }

    public string GetCollectionNameForWrite(TEntity entity)
    {
        var indexName = _indexSettingOptions.IndexPrefix + "." + typeof(TEntity).Name;
        List<ShardProviderEntity<TEntity>> sahrdEntitys = GetShardingKeyByEntity(typeof(TEntity));
        if (sahrdEntitys is null || sahrdEntitys.Count == 0)
        {
            return indexName;
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
        return indexName;
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

    public string GetCollectionNameForRead(Dictionary<string, object> conditions)
    {
        var indexName = _indexSettingOptions.IndexPrefix + "." + typeof(TEntity).Name;
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(typeof(TEntity));
        if (entitys is null || entitys.Count == 0)
        {
            return indexName;
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

        return indexName;
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
        object? getPropertyFunc = providerObj.GetType().GetField("_getPropertyFunc").GetValue(providerObj);
        _getPropertyFunc = (List<ShardProviderEntity<TEntity>>)getPropertyFunc;
        _getPropertyFunc.Sort(new ShardProviderEntityComparer<TEntity>());
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