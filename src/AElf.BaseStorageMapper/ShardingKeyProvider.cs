using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.InteropServices;
using AElf.BaseStorageMapper.Options;
using Microsoft.Extensions.Options;

namespace AElf.BaseStorageMapper;

public class ShardingKeyProvider<TEntity> : IShardingKeyProvider<TEntity> where TEntity : class, new()
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

    public void SetShardingKey(string keyName, string step, int order, Expression body, ReadOnlyCollection<ParameterExpression> parameterExpressions)
    {
        var expression = Expression.Lambda<Func<TEntity, object>>(
            Expression.Convert(body, typeof(object)), parameterExpressions);
        var func = expression.Compile();
        if (_getPropertyFunc is null)
        {
            _getPropertyFunc = new List<ShardProviderEntity<TEntity>>();
            _getPropertyFunc.Add(new ShardProviderEntity<TEntity>(keyName, step.ToString(), order, func));
        }else
        {
            _getPropertyFunc.Add(new ShardProviderEntity<TEntity>(keyName,step.ToString(), order, func));
        }
    }
    

    public ShardProviderEntity<TEntity> GetShardingKeyByEntityAndFieldName(TEntity entity, string fieldName)
    {
        return GetShardingKeyByEntity(entity).Find(a=>a.SharKeyName==fieldName);
    }

    public List<ShardProviderEntity<TEntity>> GetShardingKeyByEntity(TEntity entity)
    {
        if ( _isShardIndex == 0 || _getPropertyFunc is null || _getPropertyFunc.Count == 0)
        {
            if (CheckCollectionType(entity.GetType()))
            {
                InitShardProvider(entity);
            }
            else
            {
                return null!;
            }
        }
        return _getPropertyFunc.FindAll(a=> a.Func(entity) != null);
    }

    public string GetCollectionName(Dictionary<string, object> conditions)
    {
        var indexName = _indexSettingOptions.IndexPrefix + "." + typeof(TEntity).Name;
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(new TEntity());
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
        List<ShardProviderEntity<TEntity>> sahrdEntitys = GetShardingKeyByEntity(new TEntity());
        if (sahrdEntitys is null || sahrdEntitys.Count == 0)
        {
            return indexName;
        }
        
        foreach (var shardEntity in sahrdEntitys)
        {
            if (shardEntity.Step == "")
            {
                indexName = "-" + shardEntity.Func(entity) ?? throw new InvalidOleVariantTypeException();
            }
            else
            {
                var value = shardEntity.Func(entity);
                indexName = "-" + int.Parse(value.ToString() ?? string.Empty) / int.Parse(shardEntity.Step);
            }
        }
        return "";
    }

    public bool IsShardingCollection(Type type)
    {
        throw new NotImplementedException();
    }

    public string GetCollectionNameForRead(Dictionary<string, object> conditions)
    {
        var indexName = _indexSettingOptions.IndexPrefix + "." + typeof(TEntity).Name;
        List<ShardProviderEntity<TEntity>> entitys = GetShardingKeyByEntity(new TEntity());
        if (entitys is null || entitys.Count == 0)
        {
            return indexName;
        }

        /*List<ShardProviderEntity<TEntity>> filterEntitys = new List<ShardProviderEntity<TEntity>>();
        foreach (var dictionary in conditions)
        {
            filterEntitys.Add(entitys.Find(a => a.SharKeyName == dictionary.Key));
        }*/

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

    
    public void InitShardProvider(TEntity entity)
    {
        Type type = entity.GetType();
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
                ShardChain? shardChain = _indexShardOptions.ShardInitSettings.Find(a => a.IndexName == type.Name)?.ShardChains.First();
                ShardKey? shardKey = shardChain?.ShardKeys.Find(a => a.Name == property.Name);
                method?.Invoke(providerObj, new object[] {property.Name, shardKey.Step, attribute.Order, propertyExpression.Body, propertyExpression.Parameters});
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

public class ShardProviderEntity<TEntity> where TEntity : class, new()
{
    public string SharKeyName { get; set; }
    public string Step { get; set; }
    
    public int Order { get; set; }
    public Func<TEntity, object> Func { get; set; }
    
    public ShardProviderEntity(string keyName, string step, int order, Func<TEntity, object> func)
    {
        SharKeyName = keyName;
        Func = func;
        Step = step;
        Order = order;
        
    }

}
public class ShardProviderEntityComparer<TEntity> : IComparer<ShardProviderEntity<TEntity>> where TEntity : class, new()
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