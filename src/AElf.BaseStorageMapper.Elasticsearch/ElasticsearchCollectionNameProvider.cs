using Nest;

namespace AElf.BaseStorageMapper.Elasticsearch;

public class ElasticsearchCollectionNameProvider<TEntity> : CollectionNameProviderBase<TEntity>
    where TEntity : class
{
    protected override List<string> GetCollectionName(List<CollectionNameCondition> conditions)
    {
        // TODO: Add sharding support
        throw new NotImplementedException();
    }

    protected override string GetCollectionNameById<TKey>(TKey id)
    {
        // TODO: Add sharding support
        throw new NotImplementedException();
    }

    protected override string FormatCollectionName(string name)
    {
        return name.ToLower();
    }
}