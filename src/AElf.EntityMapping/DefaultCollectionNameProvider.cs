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

    // TODO: He should also depend on shard logic.
    private string GetDefaultCollectionName()
    {
        return typeof(TEntity).Name;
    }

    protected override async Task<List<string>> GetCollectionName(List<CollectionNameCondition> conditions)
    {
        return new List<string> { GetDefaultCollectionName() };
    }

    protected override async Task<List<string>> GetCollectionNameByEntity(TEntity entity)
    {
        return new List<string> { GetDefaultCollectionName() };
    }

    protected override async Task<List<string>> GetCollectionNameByEntity(List<TEntity> entity)
    {
        return new List<string> { GetDefaultCollectionName() };
    }

    protected override async Task<string> GetCollectionNameById<TKey>(TKey id)
    {
        return GetDefaultCollectionName();
    }

    protected override string FormatCollectionName(string name)
    {
        return name;
    }
}