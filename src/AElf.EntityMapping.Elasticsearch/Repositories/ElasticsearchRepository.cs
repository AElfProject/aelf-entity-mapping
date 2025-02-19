using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Exceptions;
using AElf.EntityMapping.Elasticsearch.Linq;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
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
    private readonly IElasticIndexService _elasticIndexService;
    private readonly IElasticsearchQueryableFactory<TEntity> _elasticsearchQueryableFactory;
    private readonly ILogger<ElasticsearchRepository<TEntity, TKey>> _logger;


    public ElasticsearchRepository(IElasticsearchClientProvider elasticsearchClientProvider,
        IOptions<AElfEntityMappingOptions> aelfEntityMappingOptions,
        ILogger<ElasticsearchRepository<TEntity, TKey>> logger,
        IOptions<ElasticsearchOptions> options, ICollectionNameProvider<TEntity> collectionNameProvider,
        IShardingKeyProvider<TEntity> shardingKeyProvider,
        IElasticIndexService elasticIndexService, IElasticsearchQueryableFactory<TEntity> elasticsearchQueryableFactory)
    {
        _logger = logger;
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _collectionNameProvider = collectionNameProvider;
        _aelfEntityMappingOptions = aelfEntityMappingOptions.Value;
        _elasticsearchOptions = options.Value;
        _shardingKeyProvider = shardingKeyProvider;
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
        await BulkAddAsync(client, indexNames, list, isSharding, cancellationToken);
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
                    var errorMessage = ElasticsearchResponseHelper.GetErrorMessage(response);
                    errorMessage = response.ItemsWithErrors.Where(item => item.Error != null).Aggregate(errorMessage, (current, item) => current + item.Error);
                    throw new ElasticsearchException(
                        $"Bulk InsertOrUpdate Document failed at index {indexNames} :{errorMessage}");
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
            var errorMessage = ElasticsearchResponseHelper.GetErrorMessage(response);
            errorMessage = response.ItemsWithErrors.Where(item => item.Error != null).Aggregate(errorMessage, (current, item) => current + item.Error);
            throw new ElasticsearchException(
                $"Bulk InsertOrUpdate Document failed at index {indexNames} :{errorMessage}");
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

        await BulkUpdateAsync(client, indexNames, list, isSharding, cancellationToken);
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

        await BulkDeleteAsync(client, indexNames, list, isSharding, cancellationToken);
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
}