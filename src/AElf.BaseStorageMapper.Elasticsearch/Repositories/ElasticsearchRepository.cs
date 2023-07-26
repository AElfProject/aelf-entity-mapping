using System.Linq.Expressions;
using AElf.BaseStorageMapper.Elasticsearch.Exceptions;
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
    private readonly ICollectionNameProvider<TEntity> _collectionNameProvider;

    public ElasticsearchRepository(IElasticsearchClientProvider elasticsearchClientProvider,
        IOptions<ElasticsearchOptions> options, ICollectionNameProvider<TEntity> collectionNameProvider)
    {
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _collectionNameProvider = collectionNameProvider;
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

    public async Task AddAsync(TEntity model, string collection = null, CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collection);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var result = await client.IndexAsync(model, ss => ss.Index(indexName).Refresh(_elasticsearchOptions.Refresh),
            cancellationToken);
        if (result.IsValid)
            return;
        throw new ElasticsearchException(
            $"Insert Document failed at index {indexName} : {result.ServerError.Error.Reason}");
    }

    public async Task AddOrUpdateAsync(TEntity model, string collection = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collection);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var exits = await client.DocumentExistsAsync(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName),
            cancellationToken);

        if (exits.Exists)
        {
            var result = await client.UpdateAsync(DocumentPath<TEntity>.Id(new Id(model)),
                ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_elasticsearchOptions.Refresh),
                cancellationToken);

            if (result.IsValid)
                return;
            throw new ElasticsearchException(
                $"Update Document failed at index {indexName} : {result.ServerError.Error.Reason}");
        }
        else
        {
            var result =
                await client.IndexAsync(model, ss => ss.Index(indexName).Refresh(_elasticsearchOptions.Refresh),
                    cancellationToken);
            if (result.IsValid)
                return;
            throw new ElasticsearchException(
                $"Insert Document failed at index {indexName} : {result.ServerError.Error.Reason}");
        }
    }

    public async Task AddOrUpdateManyAsync(List<TEntity> list, string collection = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collection);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var bulk = new BulkRequest(indexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        foreach (var item in list)
        {
            bulk.Operations.Add(new BulkIndexOperation<TEntity>(item));
        }

        var response = await client.BulkAsync(bulk, cancellationToken);
        if (!response.IsValid)
        {
            throw new ElasticsearchException(
                $"Bulk InsertOrUpdate Document failed at index {indexName} :{response.ServerError.Error.Reason}");
        }
    }

    public async Task UpdateAsync(TEntity model, string collection = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collection);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var result = await client.UpdateAsync(DocumentPath<TEntity>.Id(new Id(model)),
            ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_elasticsearchOptions.Refresh),
            cancellationToken);

        if (result.IsValid)
            return;
        throw new ElasticsearchException(
            $"Update Document failed at index {indexName} : {result.ServerError.Error.Reason}");
    }

    public async Task DeleteAsync(TKey id, string collection = null, CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collection);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response =
            await client.DeleteAsync(
                new DeleteRequest(indexName, new Id(new { id = id.ToString() }))
                    { Refresh = _elasticsearchOptions.Refresh }, cancellationToken);
        if (response.ServerError == null)
        {
            return;
        }

        throw new Exception($"Delete Document at index {indexName} :{response.ServerError.Error.Reason}");
    }

    public async Task DeleteAsync(TEntity model, string collection = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collection);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response =
            await client.DeleteAsync(
                new DeleteRequest(indexName, new Id(model)) { Refresh = _elasticsearchOptions.Refresh },
                cancellationToken);
        if (response.ServerError == null)
        {
            return;
        }

        throw new Exception($"Delete Document at index {indexName} :{response.ServerError.Error.Reason}");
    }

    public async Task DeleteManyAsync(List<TEntity> list, string collection = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collection);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var bulk = new BulkRequest(indexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        foreach (var item in list)
        {
            bulk.Operations.Add(new BulkDeleteOperation<TEntity>(new Id(item)));
        }

        var response = await client.BulkAsync(bulk, cancellationToken);
        if (response.ServerError == null)
        {
            return;
        }

        throw new ElasticsearchException(
            $"Bulk Delete Document at index {indexName} :{response.ServerError.Error.Reason}");
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

    private string GetCollectionName(string collection)
    {
        return !string.IsNullOrWhiteSpace(collection) ? collection : _collectionNameProvider.GetFullCollectionName();
    }
}