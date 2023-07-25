using System.Linq.Expressions;
using AElf.BaseStorageMapper.Elasticsearch.Linq;
using Microsoft.Extensions.Options;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Elasticsearch.Repositories;

public class ElasticsearchRepository<TEntity, TKey> : IElasticsearchRepository<TEntity, TKey>
    where TEntity : class, IEntity<TKey>
{
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ElasticsearchOptions _elasticsearchOptions;

    public ElasticsearchRepository(IElasticsearchClientProvider elasticsearchClientProvider,
        IOptions<ElasticsearchOptions> options)
    {
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _elasticsearchOptions = options.Value;
    }

    public Task<TEntity> GetAsync(TKey id, string collection = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<List<TEntity>> GetListAsync(string collection = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<long> GetCountAsync(string collection = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public async Task<IQueryable<TEntity>> GetQueryableAsync(string collection = null,
        CancellationToken cancellationToken = default)
    {
        return await GetElasticsearchQueryableAsync(collection, cancellationToken);
    }

    public Task<List<TEntity>> GetListAsync(Expression<Func<TEntity, bool>> predicate, string collection = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<long> GetCountAsync(Expression<Func<TEntity, bool>> predicate, string collection = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task AddAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task AddOrUpdateAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task AddOrUpdateManyAsync(List<TEntity> list, string collection = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task UpdateAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task DeleteAsync(TKey id, string collection = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task DeleteAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task DeleteManyAsync(List<TEntity> list, string collection = null,
        CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public Task<IElasticClient> GetElasticsearchClientAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_elasticsearchClientProvider.GetClient());
    }

    public async Task<IElasticsearchQueryable<TEntity>> GetElasticsearchQueryableAsync(string collection,
        CancellationToken cancellationToken = default)
    {
        var client = await GetElasticsearchClientAsync(cancellationToken);
        return client.AsQueryable<TEntity>(collection);
    }
}