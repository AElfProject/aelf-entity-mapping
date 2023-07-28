namespace AElf.BaseStorageMapper;

public class DefaultCollectionNameProvider<TEntity>:CollectionNameProviderBase<TEntity>
    where TEntity : class, new()
{
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;
    
    public DefaultCollectionNameProvider(IShardingKeyProvider<TEntity> shardingKeyProvider)
    {
        _shardingKeyProvider = shardingKeyProvider;
    }
    
    protected override string GetCollectionName()
    {
        return typeof(TEntity).Name;
    }

    protected override string FormatCollectionName(string name)
    {
        return name;
    }
}