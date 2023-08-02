using AElf.EntityMapping.Elasticsearch.Linq;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch;

public static class IElasticClientExtensions
{
    public static IElasticsearchQueryable<TEntity> AsQueryable<TEntity>(this IElasticClient elasticClient,
        ICollectionNameProvider<TEntity> collectionNameProvider, string index = null)
        where TEntity : class, IEntity
    {
        return new ElasticsearchQueryable<TEntity>(elasticClient, collectionNameProvider, index);
    }
}