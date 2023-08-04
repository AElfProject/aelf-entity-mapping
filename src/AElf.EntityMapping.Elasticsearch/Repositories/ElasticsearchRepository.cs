using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Exceptions;
using AElf.EntityMapping.Elasticsearch.Linq;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Options;
using Nest;
using Volo.Abp.DependencyInjection;
using Volo.Abp.Domain.Entities;
using Volo.Abp.Threading;

namespace AElf.EntityMapping.Elasticsearch.Repositories;

public class ElasticsearchRepository<TEntity, TKey> : IElasticsearchRepository<TEntity, TKey>
    where TEntity : class, IEntity<TKey>
{
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ElasticsearchOptions _elasticsearchOptions;
    private readonly ICollectionNameProvider<TEntity> _collectionNameProvider;
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;
    private readonly INonShardKeyRouteProvider<TEntity> _nonShardKeyRouteProvider;
    private readonly IElasticIndexService _elasticIndexService;
    private List<CollectionMarkField> _nonShardKeys;
    /*public IAbpLazyServiceProvider LazyServiceProvider { get; set; }
    public IElasticsearchRepository<NonShardKeyRouteCollection,string> _nonShardKeyRouteIndexRepository => LazyServiceProvider
        .LazyGetRequiredService<ElasticsearchRepository<NonShardKeyRouteCollection,string>>();*/
    

    public ElasticsearchRepository(IElasticsearchClientProvider elasticsearchClientProvider,
        IOptions<ElasticsearchOptions> options, ICollectionNameProvider<TEntity> collectionNameProvider,
        IShardingKeyProvider<TEntity> shardingKeyProvider, INonShardKeyRouteProvider<TEntity> nonShardKeyRouteProvider,
        IElasticIndexService elasticIndexService)
    {
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _collectionNameProvider = collectionNameProvider;
        _elasticsearchOptions = options.Value;
        _shardingKeyProvider = shardingKeyProvider;
        _nonShardKeyRouteProvider = nonShardKeyRouteProvider;
        _elasticIndexService = elasticIndexService;
        
        InitializeNonShardKeys();
    }
    
    private void InitializeNonShardKeys()
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            return;
        }
        if (_nonShardKeys == null)
        {
            AsyncHelper.RunSync(async () =>
            {
                _nonShardKeys = await _nonShardKeyRouteProvider.GetNonShardKeysAsync();
            });
        }
    }

    public async Task<TEntity> GetAsync(TKey id, string collectionName = null, CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionNameById(id, collectionName);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var selector = new Func<GetDescriptor<TEntity>, IGetRequest>(s => s
            .Index(indexName));
        var result = new GetResponse<TEntity>();
        try
        {
            result = await client.GetAsync(new Nest.DocumentPath<TEntity>(new Id(new {id = id.ToString()})),
                selector, cancellationToken);
        }
        catch (Exception e)
        {
            throw new EntityNotFoundException(id.ToString(), e);
        }

        
        return result.Found ? result.Source : null;
    }
    
    public async Task<List<TEntity>> GetListAsync(string collectionName = null, CancellationToken cancellationToken = default)
    {
        var queryable = await GetElasticsearchQueryableAsync(collectionName, cancellationToken);
        return queryable.ToList();
    }

    public async Task<long> GetCountAsync(string collectionName = null, CancellationToken cancellationToken = default)
    {
        var queryable = await GetElasticsearchQueryableAsync(collectionName, cancellationToken);
        return queryable.Count();
    }

    public async Task<IQueryable<TEntity>> GetQueryableAsync(string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        return await GetElasticsearchQueryableAsync(collectionName, cancellationToken);
    }

    public async Task<List<TEntity>> GetListAsync(Expression<Func<TEntity, bool>> predicate, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var queryable = await GetElasticsearchQueryableAsync(collectionName, cancellationToken);
        return queryable.Where(predicate).ToList();
    }

    public async Task<long> GetCountAsync(Expression<Func<TEntity, bool>> predicate, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var queryable = await GetElasticsearchQueryableAsync(collectionName, cancellationToken);
        return queryable.Where(predicate).Count();
    }

    public async Task AddAsync(TEntity model, string collectionName = null, CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collectionName, model);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var result = await client.IndexAsync(model, ss => ss.Index(indexName).Refresh(_elasticsearchOptions.Refresh),
            cancellationToken);

        await AddNonShardKeyRoute(model, indexName, client, cancellationToken);
        
        if (result.IsValid)
            return;
        throw new ElasticsearchException(
            $"Insert Document failed at index {indexName} : {result.ServerError.Error.Reason}");
    }

    public async Task AddOrUpdateAsync(TEntity model, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collectionName, model);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var exits = await client.DocumentExistsAsync(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName),
            cancellationToken);

        if (exits.Exists)
        {
            var result = await client.UpdateAsync(DocumentPath<TEntity>.Id(new Id(model)),
                ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_elasticsearchOptions.Refresh),
                cancellationToken);

            await UpdateNonShardKeyRoute(model, client, cancellationToken);
            
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
            
            await AddNonShardKeyRoute(model, indexName, client, cancellationToken);
            
            if (result.IsValid)
                return;
            throw new ElasticsearchException(
                $"Insert Document failed at index {indexName} : {result.ServerError.Error.Reason}");
        }
    }

    public async Task AddOrUpdateManyAsync(List<TEntity> list, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collectionName);
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
        
        //bulk index non shard key to route collection 
        if (_nonShardKeys!=null && _nonShardKeys.Any() && _elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            foreach (var nonShardKey in _nonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteBulk = new BulkRequest(nonShardKeyRouteIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
                foreach (var item in list)
                {
                    var value = item.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(item);
                    var nonShardKeyRouteIndexModel = new NonShardKeyRouteCollection()
                    {
                        Id = item.Id.ToString(),
                        ShardCollectionName = indexName,
                        // SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType)
                        SearchKey = value?.ToString()
                    };
                    nonShardKeyRouteBulk.Operations.Add(
                        new BulkIndexOperation<NonShardKeyRouteCollection>(nonShardKeyRouteIndexModel));

                }

                var nonShardKeyRouteResponse = await client.BulkAsync(nonShardKeyRouteBulk, cancellationToken);
            }
        }
        
        if (!response.IsValid)
        {
            throw new ElasticsearchException(
                $"Bulk InsertOrUpdate Document failed at index {indexName} :{response.ServerError.Error.Reason}");
        }
    }

    public async Task UpdateAsync(TEntity model, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collectionName);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var result = await client.UpdateAsync(DocumentPath<TEntity>.Id(new Id(model)),
            ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_elasticsearchOptions.Refresh),
            cancellationToken);
        
        await UpdateNonShardKeyRoute(model, client, cancellationToken);

        if (result.IsValid)
            return;
        throw new ElasticsearchException(
            $"Update Document failed at index {indexName} : {result.ServerError.Error.Reason}");
    }

    public async Task DeleteAsync(TKey id, string collectionName = null, CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collectionName);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response =
            await client.DeleteAsync(
                new DeleteRequest(indexName, new Id(new { id = id.ToString() }))
                    { Refresh = _elasticsearchOptions.Refresh }, cancellationToken);

        await DeleteNonShardKeyRoute(id.ToString(), client, cancellationToken);

        if (response.ServerError == null)
        {
            return;
        }

        throw new ElasticsearchException($"Delete Document at index {indexName} :{response.ServerError.Error.Reason}");
    }

    public async Task DeleteAsync(TEntity model, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collectionName);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response =
            await client.DeleteAsync(
                new DeleteRequest(indexName, new Id(model)) { Refresh = _elasticsearchOptions.Refresh },
                cancellationToken);
        
        await DeleteNonShardKeyRoute(model.Id.ToString(), client, cancellationToken);
        
        if (response.ServerError == null)
        {
            return;
        }

        throw new ElasticsearchException($"Delete Document at index {indexName} :{response.ServerError.Error.Reason}");
    }

    public async Task DeleteManyAsync(List<TEntity> list, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = GetCollectionName(collectionName);
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

        //bulk delete non shard key to route collection
        if (_nonShardKeys!=null && _nonShardKeys.Any() && _elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            foreach (var nonShardKey in _nonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteBulk = new BulkRequest(nonShardKeyRouteIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
                foreach (var item in list)
                {
                    nonShardKeyRouteBulk.Operations.Add(new BulkDeleteOperation<NonShardKeyRouteCollection>(new Id(item)));
                }
                
                var nonShardKeyRouteResponse = await client.BulkAsync(nonShardKeyRouteBulk, cancellationToken);
            }
        }

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

    public async Task<IElasticsearchQueryable<TEntity>> GetElasticsearchQueryableAsync(string collectionName,
        CancellationToken cancellationToken = default)
    {
        var client = await GetElasticsearchClientAsync(cancellationToken);
        return client.AsQueryable<TEntity>(_collectionNameProvider, collectionName);
    }

    private string GetCollectionName(string collection)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? collection
            : IndexNameHelper.FormatIndexName(_collectionNameProvider.GetFullCollectionName(null));
    }
    
    private string GetCollectionName(string collection, TEntity entity)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? collection
            : IndexNameHelper.FormatIndexName(_collectionNameProvider.GetFullCollectionNameByEntity(entity));
    }
    
    private string GetCollectionNameById(TKey id, string collection = null)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? collection
            : _collectionNameProvider.GetFullCollectionNameById(id);
    }

    private async Task AddNonShardKeyRoute(TEntity model, string indexName, IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            return;
        }
        
        if (_nonShardKeys!=null && _nonShardKeys.Any())
        {
            foreach (var nonShardKey in _nonShardKeys)
            {
                var value = model.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(model);

                var nonShardKeyRouteIndexModel = new NonShardKeyRouteCollection()
                {
                    Id = model.Id.ToString(),
                    ShardCollectionName = indexName,
                    // SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType)
                    SearchKey = value?.ToString()
                };

                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteResult = await client.IndexAsync(nonShardKeyRouteIndexModel,
                    ss => ss.Index(nonShardKeyRouteIndexName).Refresh(_elasticsearchOptions.Refresh),
                    cancellationToken);

            }
        }
    }

    private async Task UpdateNonShardKeyRoute(TEntity model, IElasticClient client,
        CancellationToken cancellationToken = default)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            return;
        }
        
        if (_nonShardKeys!=null && _nonShardKeys.Any())
        {
            foreach (var nonShardKey in _nonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteIndexId = model.Id.ToString();
                var nonShardKeyRouteIndexModel =
                    await _nonShardKeyRouteProvider.GetNonShardKeyRouteIndexAsync(nonShardKeyRouteIndexId,
                        nonShardKeyRouteIndexName);
                // var nonShardKeyRouteIndexModel = GetAsync((TKey)Convert.ChangeType(nonShardKeyRouteIndexId, typeof(TKey)), nonShardKeyRouteIndexName)  as NonShardKeyRouteCollection;

                var value = model.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(model);
                if (nonShardKeyRouteIndexModel != null)
                {
                    // nonShardKeyRouteIndexModel.SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType);
                    nonShardKeyRouteIndexModel.SearchKey = value?.ToString();

                    var nonShardKeyRouteResult = await client.UpdateAsync(
                        DocumentPath<NonShardKeyRouteCollection>.Id(new Id(nonShardKeyRouteIndexModel)),
                        ss => ss.Index(nonShardKeyRouteIndexName).Doc(nonShardKeyRouteIndexModel).RetryOnConflict(3)
                            .Refresh(_elasticsearchOptions.Refresh),
                        cancellationToken);
                }
            }
        }
    }

    private async Task DeleteNonShardKeyRoute(string id, IElasticClient client,
        CancellationToken cancellationToken = default)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            return;
        }
        if (_nonShardKeys!=null && _nonShardKeys.Any())
        {
            foreach (var nonShardKey in _nonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteIndexId = id;
                var nonShardKeyRouteResult=await client.DeleteAsync(
                    new DeleteRequest(nonShardKeyRouteIndexName, new Id(new { id = nonShardKeyRouteIndexId.ToString() }))
                        { Refresh = _elasticsearchOptions.Refresh }, cancellationToken);
            }
        }
    }


}