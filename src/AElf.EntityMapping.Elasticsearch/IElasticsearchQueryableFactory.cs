using AElf.EntityMapping.Elasticsearch.Linq;
using AElf.EntityMapping.Elasticsearch.Options;
using Microsoft.Extensions.Options;
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
    private readonly ElasticsearchOptions _elasticsearchOptions;

    public ElasticsearchQueryableFactory(ICollectionNameProvider<TEntity> collectionNameProvider,
        IOptions<ElasticsearchOptions> elasticsearchOptions)
    {
        _collectionNameProvider = collectionNameProvider;
        _elasticsearchOptions = elasticsearchOptions.Value;
    }

    public ElasticsearchQueryable<TEntity> Create(IElasticClient client,
        string index = null)
    {
        return new ElasticsearchQueryable<TEntity>(client, _collectionNameProvider, index, _elasticsearchOptions);
    }
}