using Nest;

namespace AElf.BaseStorageMapper.Elasticsearch;

public class ElasticsearchCollectionNameProvider<TEntity> : CollectionNameProviderBase<TEntity>
    where TEntity : class
{
    protected override string GetCollectionName()
    {
        // TODO: Add sharding support
        throw new NotImplementedException();
    }

    protected override string FormatCollectionName(string name)
    {
        return name.ToLower();
    }
}