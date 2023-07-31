using AElf.BaseStorageMapper.Elasticsearch.Linq;
using AElf.BaseStorageMapper.Sharding;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Elasticsearch;

public static class IElasticClientExtensions
{
    public static IElasticsearchQueryable<TEntity> AsQueryable<TEntity>(this IElasticClient elasticClient,
        ICollectionNameProvider<TEntity> collectionNameProvider, string index = null)
        where TEntity : class, IEntity
    {
        return new ElasticsearchQueryable<TEntity>(elasticClient, collectionNameProvider, index);
    }
}