namespace AElf.BaseStorageMapper;

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
        return string.IsNullOrWhiteSpace(AElfBaseStorageMapperOptions.CollectionPrefix)
        ? typeof(TEntity).Name
        : $"{AElfBaseStorageMapperOptions.CollectionPrefix}.{typeof(TEntity).Name}";
    }

    protected override List<string> GetCollectionName(List<CollectionNameCondition> conditions)
    {
        return new List<string> { GetDefaultCollectionName() };
    }

    protected override string GetCollectionNameById<TKey>(TKey id)
    {
        return typeof(TEntity).Name;
    }

    protected override string FormatCollectionName(string name)
    {
        return name;
    }
}