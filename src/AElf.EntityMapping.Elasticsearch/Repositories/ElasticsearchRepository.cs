using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Exceptions;
using AElf.EntityMapping.Elasticsearch.Linq;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Repositories;

public class ElasticsearchRepository<TEntity, TKey> : IElasticsearchRepository<TEntity, TKey>
    where TEntity : class, IEntity<TKey>
{
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly AElfEntityMappingOptions _aelfEntityMappingOptions;
    private readonly ElasticsearchOptions _elasticsearchOptions;
    private readonly ICollectionNameProvider<TEntity> _collectionNameProvider;
    private readonly IShardingKeyProvider<TEntity> _shardingKeyProvider;
    private readonly ICollectionRouteKeyProvider<TEntity> _collectionRouteKeyProvider;
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IElasticsearchQueryableFactory<TEntity> _elasticsearchQueryableFactory;
    private readonly ILogger<ElasticsearchRepository<TEntity, TKey>> _logger;


    public ElasticsearchRepository(IElasticsearchClientProvider elasticsearchClientProvider,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions,
        ILogger<ElasticsearchRepository<TEntity, TKey>> logger,
        IOptions<ElasticsearchOptions> options, ICollectionNameProvider<TEntity> collectionNameProvider,
        IShardingKeyProvider<TEntity> shardingKeyProvider, ICollectionRouteKeyProvider<TEntity> collectionRouteKeyProvider,
        IElasticIndexService elasticIndexService, IElasticsearchQueryableFactory<TEntity> elasticsearchQueryableFactory)
    {
        _logger = logger;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _collectionNameProvider = collectionNameProvider;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _elasticsearchOptions = options.Value;
        _shardingKeyProvider = shardingKeyProvider;
        _collectionRouteKeyProvider = collectionRouteKeyProvider;
        _elasticIndexService = elasticIndexService;
        _elasticsearchQueryableFactory = elasticsearchQueryableFactory;
    }

    public async Task<TEntity> GetAsync(TKey id, string collectionName = null, CancellationToken cancellationToken = default)
    {
        var indexName = await GetFullCollectionNameByIdAsync(id, collectionName);
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
        var indexName = await GetFullCollectionNameAsync(collectionName, model);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var result = await client.IndexAsync(model, ss => ss.Index(indexName).Refresh(_elasticsearchOptions.Refresh),
            cancellationToken);

        await _collectionRouteKeyProvider.AddCollectionRouteKeyAsync(model, indexName, cancellationToken);
        
        if (result.IsValid)
            return;
        throw new ElasticsearchException(
            $"Insert Document failed at index {indexName} : {result.ServerError.Error.Reason}");
    }

    public async Task AddOrUpdateAsync(TEntity model, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = await GetFullCollectionNameAsync(collectionName, model);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var exits = await client.DocumentExistsAsync(DocumentPath<TEntity>.Id(new Id(model)), dd => dd.Index(indexName),
            cancellationToken);

        if (exits.Exists)
        {
            var result = await client.UpdateAsync(DocumentPath<TEntity>.Id(new Id(model)),
                ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_elasticsearchOptions.Refresh),
                cancellationToken);

            await _collectionRouteKeyProvider.UpdateCollectionRouteKeyAsync(model, cancellationToken);
            
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
            
            await _collectionRouteKeyProvider.AddCollectionRouteKeyAsync(model, indexName, cancellationToken);
            
            if (result.IsValid)
                return;
            throw new ElasticsearchException(
                $"Insert Document failed at index {indexName} : {result.ServerError.Error.Reason}");
        }
    }

    public async Task AddOrUpdateManyAsync(List<TEntity> list, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexNames = await GetFullCollectionNameAsync(collectionName, list);
        var isSharding = _shardingKeyProvider.IsShardingCollection();

        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response = new BulkResponse();
        var bulk = new BulkRequest()
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };

        for (int i = 0; i < list.Count; i++)
        {
            var operation = new BulkIndexOperation<TEntity>(list[i]);
            operation.Index = indexNames[i];
            bulk.Operations.Add(operation);
        }
        
        //bulk index non shard key to route collection 
        var routeKeyBulkOperationList =
            await GetBulkAddMCollectionRouteKeyOperationsAsync(list, indexNames, cancellationToken);
        bulk.Operations.AddRange(routeKeyBulkOperationList);
        _logger.LogDebug("Before BulkAsync time: {0} ", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
        response = await client.BulkAsync(bulk, cancellationToken);
        _logger.LogDebug("After BulkAsync time: {0} ", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"));
        if (!response.IsValid)
        {
            throw new ElasticsearchException(
                $"Bulk InsertOrUpdate Document failed at index {indexNames} :{response.ServerError.Error.Reason}");
        }
    }

    public async Task UpdateAsync(TEntity model, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = await GetFullCollectionNameAsync(collectionName,model);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var result = await client.UpdateAsync(DocumentPath<TEntity>.Id(new Id(model)),
            ss => ss.Index(indexName).Doc(model).RetryOnConflict(3).Refresh(_elasticsearchOptions.Refresh),
            cancellationToken);
        
        await _collectionRouteKeyProvider.UpdateCollectionRouteKeyAsync(model, cancellationToken);

        if (result.IsValid)
            return;
        throw new ElasticsearchException(
            $"Update Document failed at index {indexName} : {result.ServerError.Error.Reason}");
    }

    public async Task DeleteAsync(TKey id, string collectionName = null, CancellationToken cancellationToken = default)
    {
        var indexName = await GetFullCollectionNameByIdAsync(id);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response =
            await client.DeleteAsync(
                new DeleteRequest(indexName, new Id(new { id = id.ToString() }))
                    { Refresh = _elasticsearchOptions.Refresh }, cancellationToken);

        await _collectionRouteKeyProvider.DeleteCollectionRouteKeyAsync(id.ToString(), cancellationToken);

        if (response.ServerError == null)
        {
            return;
        }

        throw new ElasticsearchException($"Delete Document at index {indexName} :{response.ServerError.Error.Reason}");
    }

    public async Task DeleteAsync(TEntity model, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexName = await GetFullCollectionNameAsync(collectionName, model);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response =
            await client.DeleteAsync(
                new DeleteRequest(indexName, new Id(model)) { Refresh = _elasticsearchOptions.Refresh },
                cancellationToken);
        
        await _collectionRouteKeyProvider.DeleteCollectionRouteKeyAsync(model.Id.ToString(), cancellationToken);
        
        if (response.ServerError == null)
        {
            return;
        }

        throw new ElasticsearchException($"Delete Document at index {indexName} :{response.ServerError.Error.Reason}");
    }

    public async Task DeleteManyAsync(List<TEntity> list, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexNames = await GetFullCollectionNameAsync(collectionName, list);
        var isSharding = _shardingKeyProvider.IsShardingCollection();
        
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response = new BulkResponse();
        var bulk = new BulkRequest()
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        
        for (int i = 0; i < list.Count; i++)
        {
            var operation = new BulkDeleteOperation<TEntity>(new Id(list[i]));
            operation.Index = indexNames[i];
            bulk.Operations.Add(operation);
        }
        
        //bulk delete non shard key to route collection
        var routeKeyBulkOperationList =
            await GetBulkDeleteCollectionRouteKeyOperationsAsync(list, cancellationToken);
        bulk.Operations.AddRange(routeKeyBulkOperationList);
        response = await client.BulkAsync(bulk, cancellationToken);
        if (response.ServerError == null)
        {
            return;
        }

        throw new ElasticsearchException(
            $"Bulk Delete Document at index {indexNames} :{response.ServerError.Error.Reason}");
    }

    public Task<IElasticClient> GetElasticsearchClientAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_elasticsearchClientProvider.GetClient());
    }

    public async Task<IElasticsearchQueryable<TEntity>> GetElasticsearchQueryableAsync(string collectionName,
        CancellationToken cancellationToken = default)
    {
        var client = await GetElasticsearchClientAsync(cancellationToken);
        return _elasticsearchQueryableFactory.Create(client, collectionName);
    }

    private async Task<string> GetFullCollectionNameAsync(string collection)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? collection
            : IndexNameHelper.FormatIndexName( await _collectionNameProvider.GetFullCollectionNameAsync(null));
    }
    
    private async Task<string> GetFullCollectionNameAsync(string collection, TEntity entity)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? collection
            : IndexNameHelper.FormatIndexName(await _collectionNameProvider.GetFullCollectionNameByEntityAsync(entity));
    }
    
    private async Task<List<string>> GetFullCollectionNameAsync(string collection, List<TEntity> entities)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? new List<string>(){collection}
            : await _collectionNameProvider.GetFullCollectionNameByEntityAsync(entities);
    }
    
    private async Task<string> GetFullCollectionNameByIdAsync(TKey id, string collection = null)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? collection
            : await _collectionNameProvider.GetFullCollectionNameByIdAsync(id);
    }

    
    private async Task<List<BulkIndexOperation<IRouteKeyCollection>>> GetBulkAddMCollectionRouteKeyOperationsAsync(List<TEntity> modelList,
        List<string> fullCollectionNameList, CancellationToken cancellationToken = default)
    {
        var collectionRouteKeys = await _collectionRouteKeyProvider.GetCollectionRouteKeyItemsAsync();
        var bulkIndexOperationList = new List<BulkIndexOperation<IRouteKeyCollection>>();
        if (collectionRouteKeys != null && collectionRouteKeys.Any() && _shardingKeyProvider.IsShardingCollection())
        {
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in collectionRouteKeys)
            {
                var collectionRouteKeyIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                        _aelfEntityMappingOptions.CollectionPrefix);
                int indexNameCount = 0;
                foreach (var item in modelList)
                {
                    // var value = item.GetType().GetProperty(collectionRouteKey.FieldName)?.GetValue(item);
                    var value = collectionRouteKey.GetRouteKeyValueFunc(item);
                    string indexName = IndexNameHelper.RemoveCollectionPrefix(fullCollectionNameList[indexNameCount],
                        _aelfEntityMappingOptions.CollectionPrefix);
                    var collectionRouteKeyIndexModel = new RouteKeyCollection()
                    {
                        Id = item.Id.ToString(),
                        CollectionName = indexName,
                        // SearchKey = Convert.ChangeType(value, collectionRouteKey.FieldValueType)
                        CollectionRouteKey = value?.ToString()
                    };
                    var operation = new BulkIndexOperation<IRouteKeyCollection>(collectionRouteKeyIndexModel);
                    operation.Index = collectionRouteKeyIndexName;
                    bulkIndexOperationList.Add(operation);
                    indexNameCount++;
                }
            }
        }

        return bulkIndexOperationList;
    }
    
    private async Task<List<BulkDeleteOperation<IRouteKeyCollection>>> GetBulkDeleteCollectionRouteKeyOperationsAsync(List<TEntity> modelList,CancellationToken cancellationToken = default)
    {
        var collectionRouteKeys = await _collectionRouteKeyProvider.GetCollectionRouteKeyItemsAsync();
        var bulkDeleteOperationList = new List<BulkDeleteOperation<IRouteKeyCollection>>();
        if (collectionRouteKeys!=null && collectionRouteKeys.Any() && _shardingKeyProvider.IsShardingCollection())
        {
            // var routeKeyTaskList = new List<Task>();
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in collectionRouteKeys)
            {
                var collectionRouteKeyRouteIndexName =
                    IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                        _aelfEntityMappingOptions.CollectionPrefix);
                foreach (var item in modelList)
                {
                    var operation = new BulkDeleteOperation<IRouteKeyCollection>(new Id(item.Id.ToString()));
                    operation.Index = collectionRouteKeyRouteIndexName;
                    bulkDeleteOperationList.Add(operation);
                }
            }
        }
        return bulkDeleteOperationList;
    }

}