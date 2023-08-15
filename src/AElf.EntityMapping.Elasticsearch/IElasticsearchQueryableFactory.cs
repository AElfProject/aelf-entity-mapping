using AElf.EntityMapping.Elasticsearch.Linq;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch;

public interface IElasticsearchQueryableFactory<TEntity>
    where TEntity : class, IEntity
{
    ElasticsearchQueryable<TEntity> Create(IElasticClient client, string index = null);
}

public class ElasticsearchQueryableFactory<TEntity> : IElasticsearchQueryableFactory<TEntity>
    where TEntity : class, IEntity
{
    private readonly ICollectionNameProvider<TEntity> _collectionNameProvider;

    public ElasticsearchQueryableFactory(ICollectionNameProvider<TEntity> collectionNameProvider)
    {
        _collectionNameProvider = collectionNameProvider;
    }

    public ElasticsearchQueryable<TEntity> Create(IElasticClient client, string index = null)
    {
        return new ElasticsearchQueryable<TEntity>(client, _collectionNameProvider, index);
    }
}