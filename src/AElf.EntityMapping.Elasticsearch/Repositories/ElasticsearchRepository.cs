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
            throw new ElasticsearchException($"Get Document failed at index {indexName} id {id.ToString()}", e);
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
            $"Insert Document failed at index {indexName} id {(model == null ? "" : model.Id.ToString())} : {ElasticsearchResponseHelper.GetErrorMessage(result)}");
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
                $"Update Document failed at index {indexName} id {(model == null ? "" : model.Id.ToString())} : {ElasticsearchResponseHelper.GetErrorMessage(result)}");
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
                $"Insert Document failed at index {indexName} id {(model == null ? "" : model.Id.ToString())} : {ElasticsearchResponseHelper.GetErrorMessage(result)}");
        }
    }

    public async Task AddOrUpdateManyAsync(List<TEntity> list, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        string entityName = typeof(TEntity).Name;
        _logger.LogDebug("[{1}]Before AddOrUpdateManyAsync time: {0} ",
            DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), entityName);
        var indexNames = await GetFullCollectionNameAsync(collectionName, list);
        _logger.LogDebug("[{1}]After GetFullCollectionNameAsync time: {0} ",
            DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), entityName);
        var isSharding = _shardingKeyProvider.IsShardingCollection();
        
        var client = await GetElasticsearchClientAsync(cancellationToken);
        if (!isSharding)
        {
            await BulkAddAsync(client, indexNames, list, isSharding, cancellationToken);
            return;
        }
        
        _logger.LogDebug("[{1}]Before GetBulkAddTaskAsync time: {0} ",
            DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
            entityName);
        var bulkAddTaskList = new List<Task>();
        bulkAddTaskList.Add(BulkAddAsync(client, indexNames, list, isSharding, cancellationToken));
        _logger.LogDebug("[{1}]After GetBulkAddTaskAsync time: {0} ",
            DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"), entityName);
        var routeKeyTaskList =
            await GetBulkAddCollectionRouteKeyTasksAsync(isSharding, list, indexNames, cancellationToken);
        if (routeKeyTaskList.Count > 0)
        {
            bulkAddTaskList.AddRange(routeKeyTaskList);
        }
        _logger.LogDebug("[{1}]Before Task.WhenAll time: {0} ", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
            entityName);
        await Task.WhenAll(bulkAddTaskList.ToArray());
        _logger.LogDebug("[{1}]After Task.WhenAll time: {0} ", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff"),
            entityName);
    }
    
    private async Task BulkAddAsync(IElasticClient client,List<string> indexNames,List<TEntity> list, bool isSharding, CancellationToken cancellationToken = default)
    {
        var response = new BulkResponse();
        var currentIndexName = indexNames[0];
        var bulk = new BulkRequest(currentIndexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };

        for (int i = 0; i < list.Count; i++)
        {
            if (isSharding && (currentIndexName != indexNames[i]))
            {
                response = await client.BulkAsync(bulk, cancellationToken);
                if (!response.IsValid)
                {
                    throw new ElasticsearchException(
                        $"Bulk InsertOrUpdate Document failed at index {indexNames} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
                }
                
                currentIndexName = indexNames[i];
                
                bulk = new BulkRequest(currentIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
            }
            bulk.Operations.Add(new BulkIndexOperation<TEntity>(list[i]));
        }
        
        response = await client.BulkAsync(bulk, cancellationToken);
        if (!response.IsValid)
        {
            throw new ElasticsearchException(
                $"Bulk InsertOrUpdate Document failed at index {indexNames} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
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
            $"Update Document failed at index {indexName} id {(model == null ? "" : model.Id.ToString())} : {ElasticsearchResponseHelper.GetErrorMessage(result)}");
    }

    public async Task UpdateManyAsync(List<TEntity> list, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexNames = await GetFullCollectionNameAsync(collectionName, list);
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var isSharding = _shardingKeyProvider.IsShardingCollection();
        if (!isSharding)
        {
            await BulkUpdateAsync(client, indexNames, list, isSharding, cancellationToken);
            return;
        }
        
        var bulkUpdateTaskList = new List<Task>();
        bulkUpdateTaskList.Add(BulkUpdateAsync(client, indexNames, list, isSharding, cancellationToken));
        var routeKeyTaskList =
            await GetBulkUpdateCollectionRouteKeyTasksAsync(isSharding, list, indexNames, cancellationToken);
        if (routeKeyTaskList.Count > 0)
        {
            bulkUpdateTaskList.AddRange(routeKeyTaskList);
        }
        await Task.WhenAll(bulkUpdateTaskList.ToArray());
    }

    private async Task BulkUpdateAsync(IElasticClient client, List<string> indexNames, List<TEntity> list, bool isSharding,
        CancellationToken cancellationToken = default)
    {
        var response = new BulkResponse();
        var currentIndexName = indexNames[0];
        var bulk = new BulkRequest(currentIndexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        for (int i = 0; i < list.Count; i++)
        {
            if (isSharding && (currentIndexName != indexNames[i]))
            {
                response = await client.BulkAsync(bulk, cancellationToken);
                if (!response.IsValid)
                {
                    throw new ElasticsearchException(
                        $"Bulk Update Document failed at index {indexNames} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
                }
                
                currentIndexName = indexNames[i];
                
                bulk = new BulkRequest(currentIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
            }
            var updateOperation = new BulkUpdateOperation<TEntity,TEntity>(new Id(list[i]))
            {
                Doc = list[i],
                Index = currentIndexName
            };
            bulk.Operations.Add(updateOperation);
        }
        
        response = await client.BulkAsync(bulk, cancellationToken);
        if (!response.IsValid)
        {
            throw new ElasticsearchException(
                $"Bulk Update Document failed at index {indexNames} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
        }
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

        throw new ElasticsearchException($"Delete Document at index {indexName} id {id.ToString()} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
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

        throw new ElasticsearchException(
            $"Delete Document at index {indexName} id {(model == null ? "" : model.Id.ToString())} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
    }

    public async Task DeleteManyAsync(List<TEntity> list, string collectionName = null,
        CancellationToken cancellationToken = default)
    {
        var indexNames = await GetFullCollectionNameAsync(collectionName, list);
        var isSharding = _shardingKeyProvider.IsShardingCollection();
        
        var client = await GetElasticsearchClientAsync(cancellationToken);
        if (!isSharding)
        {
            await BulkDeleteAsync(client, indexNames, list, isSharding, cancellationToken);
            return;
        }
        
        var bulkDeleteTaskList = new List<Task>();
        bulkDeleteTaskList.Add(BulkDeleteAsync(client, indexNames, list, isSharding, cancellationToken));
        var routeKeyTaskList =
            await GetBulkDeleteCollectionRouteKeyTasksAsync(isSharding, list, cancellationToken);
        if (routeKeyTaskList.Count > 0)
        {
            bulkDeleteTaskList.AddRange(routeKeyTaskList);
        }
        await Task.WhenAll(bulkDeleteTaskList.ToArray());

    }

    private async Task BulkDeleteAsync(IElasticClient client, List<string> indexNames, List<TEntity> list, bool isSharding,
        CancellationToken cancellationToken = default)
    {
        var response = new BulkResponse();
        var currentIndexName = indexNames[0];
        var bulk = new BulkRequest(currentIndexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        
        for (int i = 0; i < list.Count; i++)
        {
            if (isSharding && (currentIndexName != indexNames[i]))
            {
                response = await client.BulkAsync(bulk, cancellationToken);
                if (response.ServerError != null)
                {
                    throw new ElasticsearchException(
                        $"Bulk Delete Document failed at index {indexNames} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
                }
                
                currentIndexName = indexNames[i];
                
                bulk = new BulkRequest(currentIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
            }
            bulk.Operations.Add(new BulkDeleteOperation<TEntity>(new Id(list[i])));
        }
        
        response = await client.BulkAsync(bulk, cancellationToken);

        if (response.ServerError == null)
        {
            return;
        }

        throw new ElasticsearchException(
            $"Bulk Delete Document at index {indexNames} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
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


    private async Task<List<Task>> GetBulkAddCollectionRouteKeyTasksAsync(bool isSharding, List<TEntity> modelList,
        List<string> fullCollectionNameList, CancellationToken cancellationToken = default)
    {
        var collectionRouteKeys = await _collectionRouteKeyProvider.GetCollectionRouteKeyItemsAsync();
        if (collectionRouteKeys != null && collectionRouteKeys.Any() && isSharding)
        {
            var routeKeyTaskList = new List<Task>();
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in collectionRouteKeys)
            {
                routeKeyTaskList.Add(BulkAddRouteKey(client, modelList, collectionRouteKey, fullCollectionNameList,
                    cancellationToken));
            }

            return routeKeyTaskList;
        }

        return new List<Task>();
    }

    private async Task BulkAddRouteKey(IElasticClient client, List<TEntity> modelList,
        CollectionRouteKeyItem<TEntity> collectionRouteKey, List<string> fullCollectionNameList,
        CancellationToken cancellationToken)
    {
        var collectionRouteKeyIndexName =
            IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                _aelfEntityMappingOptions.CollectionPrefix);
        var collectionRouteKeyBulk = new BulkRequest(collectionRouteKeyIndexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
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
            collectionRouteKeyBulk.Operations.Add(
                new BulkIndexOperation<RouteKeyCollection>(collectionRouteKeyIndexModel));
            indexNameCount++;
        }

        var response = await client.BulkAsync(collectionRouteKeyBulk, cancellationToken);
        if (!response.IsValid)
        {
            throw new ElasticsearchException(
                $"Bulk InsertOrUpdate Document failed at index {collectionRouteKeyIndexName} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
        }
    }

    private async Task<List<Task>> GetBulkUpdateCollectionRouteKeyTasksAsync(bool isSharding, List<TEntity> modelList,
        List<string> fullCollectionNameList, CancellationToken cancellationToken = default)
    {
        var collectionRouteKeys = await _collectionRouteKeyProvider.GetCollectionRouteKeyItemsAsync();
        if (collectionRouteKeys != null && collectionRouteKeys.Any() && isSharding)
        {
            var routeKeyTaskList = new List<Task>();
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in collectionRouteKeys)
            {
                routeKeyTaskList.Add(BulkUpdateRouteKey(client, modelList, collectionRouteKey, fullCollectionNameList,
                    cancellationToken));
            }

            return routeKeyTaskList;
        }

        return new List<Task>();
    }

    private async Task BulkUpdateRouteKey(IElasticClient client, List<TEntity> modelList,
        CollectionRouteKeyItem<TEntity> collectionRouteKey, List<string> fullCollectionNameList,
        CancellationToken cancellationToken)
    {
        var collectionRouteKeyIndexName =
            IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                _aelfEntityMappingOptions.CollectionPrefix);
        var collectionRouteKeyBulk = new BulkRequest(collectionRouteKeyIndexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
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
            var updateOperation = new BulkUpdateOperation<RouteKeyCollection,RouteKeyCollection>(new Id(collectionRouteKeyIndexModel))
            {
                Doc = collectionRouteKeyIndexModel,
                Index = collectionRouteKeyIndexName
            };
            collectionRouteKeyBulk.Operations.Add(updateOperation);
            indexNameCount++;
        }

        var response = await client.BulkAsync(collectionRouteKeyBulk, cancellationToken);
        if (!response.IsValid)
        {
            throw new ElasticsearchException(
                $"Bulk Update Document failed at index {collectionRouteKeyIndexName} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
        }
    }

    private async Task<List<Task>> GetBulkDeleteCollectionRouteKeyTasksAsync(bool isSharding, List<TEntity> modelList,
        CancellationToken cancellationToken = default)
    {
        var collectionRouteKeys = await _collectionRouteKeyProvider.GetCollectionRouteKeyItemsAsync();
        if (collectionRouteKeys != null && collectionRouteKeys.Any() && isSharding)
        {
            var routeKeyTaskList = new List<Task>();
            var client = await GetElasticsearchClientAsync(cancellationToken);
            foreach (var collectionRouteKey in collectionRouteKeys)
            {
                routeKeyTaskList.Add(BulkDeleteRouteKey(client, modelList, collectionRouteKey, cancellationToken));
            }
            
            return routeKeyTaskList;
        }

        return new List<Task>();
    }

    private async Task BulkDeleteRouteKey(IElasticClient client, List<TEntity> modelList,
        CollectionRouteKeyItem<TEntity> collectionRouteKey, CancellationToken cancellationToken)
    {
        var collectionRouteKeyRouteIndexName =
            IndexNameHelper.GetCollectionRouteKeyIndexName(typeof(TEntity), collectionRouteKey.FieldName,
                _aelfEntityMappingOptions.CollectionPrefix);
        var collectionRouteKeyRouteBulk = new BulkRequest(collectionRouteKeyRouteIndexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        foreach (var item in modelList)
        {
            collectionRouteKeyRouteBulk.Operations.Add(new BulkDeleteOperation<RouteKeyCollection>(new Id(item)));
        }

        var response = await client.BulkAsync(collectionRouteKeyRouteBulk, cancellationToken);
        
        if (response.ServerError == null)
        {
            return;
        }

        throw new ElasticsearchException(
            $"Bulk Delete Document at index {collectionRouteKeyRouteIndexName} :{ElasticsearchResponseHelper.GetErrorMessage(response)}");
    }

}