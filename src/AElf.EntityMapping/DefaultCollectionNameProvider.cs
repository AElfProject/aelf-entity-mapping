using AElf.EntityMapping.Sharding;

namespace AElf.EntityMapping;

public class DefaultCollectionNameProvider<TEntity> : CollectionNameProviderBase<TEntity>
    where TEntity : class, new()
{
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;

    public DefaultCollectionNameProvider(IShardingKeyProvider<TEntity> shardingKeyProvider)
    {
        _shardingKeyProvider = shardingKeyProvider;
    }

    private string GetDefaultCollectionName()
    {
        return typeof(TEntity).Name;
    }

    protected override List<string> GetCollectionName(List<CollectionNameCondition> conditions)
    {
        return new List<string> { GetDefaultCollectionName() };
    }

    protected override List<string> GetCollectionNameByEntity(TEntity entity)
    {
        return new List<string> { GetDefaultCollectionName() };
    }

    protected override string GetCollectionNameById<TKey>(TKey id)
    {
        return GetDefaultCollectionName();
    }

    protected override string FormatCollectionName(string name)
    {
        return name;
    }
}