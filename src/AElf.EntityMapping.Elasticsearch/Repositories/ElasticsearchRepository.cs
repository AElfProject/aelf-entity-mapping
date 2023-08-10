using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Exceptions;
using AElf.EntityMapping.Elasticsearch.Linq;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Services;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Options;
using Nest;
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
    // private List<CollectionMarkField> _nonShardKeys;
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
        
        // InitializeNonShardKeys();
    }
    
    // private void InitializeNonShardKeys()
    // {
    //     if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
    //     {
    //         return;
    //     }
    //     if (_nonShardKeys == null)
    //     {
    //         AsyncHelper.RunSync(async () =>
    //         {
    //             _nonShardKeys = await _nonShardKeyRouteProvider.GetNonShardKeysAsync();
    //         });
    //     }
    // }

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

        await AddNonShardKeyRoute(model, indexName, client, cancellationToken);
        
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
        //var indexName = GetCollectionNameAsync(collectionName);
        var indexNames = await GetFullCollectionNameAsync(collectionName, list);
        var isSharding = _elasticIndexService.IsShardingCollection(typeof(TEntity));
        /*var client = await GetElasticsearchClientAsync(cancellationToken);
        var bulk = new BulkRequest(indexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        foreach (var item in list)
        {
            bulk.Operations.Add(new BulkIndexOperation<TEntity>(item));
        }

        var response = await client.BulkAsync(bulk, cancellationToken);*/
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response = new BulkResponse();
        var bulk = new BulkRequest();
        var currentIndexName = string.Empty;
        // List<BulkIndexOperation<TEntity>> operations = new List<BulkIndexOperation<TEntity>>();
        
        for (int i = 0; i < list.Count; i++)
        {
            // response = await client.IndexAsync(list[i], ss => ss.Index(indexNames[i]).Refresh(_elasticsearchOptions.Refresh),
            //     cancellationToken);

            if (string.IsNullOrEmpty(currentIndexName))
            {
                if (isSharding)
                {
                    currentIndexName = indexNames[i];
                }
                else
                {
                    currentIndexName = indexNames[0];
                }
                
                bulk = new BulkRequest(currentIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
            }
            
            if (isSharding && (currentIndexName != indexNames[i]))
            {
                response = await client.BulkAsync(bulk, cancellationToken);
                if (!response.IsValid)
                {
                    throw new ElasticsearchException(
                        $"Bulk InsertOrUpdate Document failed at index {indexNames} :{response.ServerError.Error.Reason}");
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

        //bulk index non shard key to route collection 
        await AddManyNonShardKeyRoute(list, indexNames, client, cancellationToken);
        
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
        
        await UpdateNonShardKeyRoute(model, client, cancellationToken);

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
        var indexName = await GetFullCollectionNameAsync(collectionName, model);
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
        var indexNames = await GetFullCollectionNameAsync(collectionName, list);
        var isSharding = _elasticIndexService.IsShardingCollection(typeof(TEntity));
        /*var client = await GetElasticsearchClientAsync(cancellationToken);
        var bulk = new BulkRequest(indexName)
        {
            Operations = new List<IBulkOperation>(),
            Refresh = _elasticsearchOptions.Refresh
        };
        foreach (var item in list)
        {
            bulk.Operations.Add(new BulkDeleteOperation<TEntity>(new Id(item)));
        }

        var response = await client.BulkAsync(bulk, cancellationToken);*/
        var client = await GetElasticsearchClientAsync(cancellationToken);
        var response = new BulkResponse();
        var bulk = new BulkRequest();
        var currentIndexName = string.Empty;
        for (int i = 0; i < list.Count; i++)
        {
            // response = await client.DeleteAsync(
            //     new DeleteRequest(indexNames[i], new Id(list[i])) { Refresh = _elasticsearchOptions.Refresh },
            //         cancellationToken);
            
            if (string.IsNullOrEmpty(currentIndexName))
            {
                if (isSharding)
                {
                    currentIndexName = indexNames[i];
                }
                else
                {
                    currentIndexName = indexNames[0];
                }

                bulk = new BulkRequest(currentIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
            }
            
            if (isSharding && (currentIndexName != indexNames[i]))
            {
                response = await client.BulkAsync(bulk, cancellationToken);
                if (!response.IsValid)
                {
                    throw new ElasticsearchException(
                        $"Bulk InsertOrUpdate Document failed at index {indexNames} :{response.ServerError.Error.Reason}");
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

        //bulk delete non shard key to route collection
        await DeleteManyNonShardKeyRoute(list, client, cancellationToken);

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
        return client.AsQueryable<TEntity>(_collectionNameProvider, collectionName);
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
    
    private async Task<List<string>> GetFullCollectionNameAsync(string collection, List<TEntity> entitys)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? new List<string>(){collection}
            : await _collectionNameProvider.GetFullCollectionNameByEntityAsync(entitys);
    }
    
    private async Task<string> GetFullCollectionNameByIdAsync(TKey id, string collection = null)
    {
        return !string.IsNullOrWhiteSpace(collection)
            ? collection
            : await _collectionNameProvider.GetFullCollectionNameByIdAsync(id);
    }

    private async Task AddManyNonShardKeyRoute(List<TEntity> modelList,List<string> fullIndexNameList, IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (_nonShardKeyRouteProvider.NonShardKeys!=null && _nonShardKeyRouteProvider.NonShardKeys.Any() && _elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            foreach (var nonShardKey in _nonShardKeyRouteProvider.NonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteBulk = new BulkRequest(nonShardKeyRouteIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
                int indexNameCount = 0;
                foreach (var item in modelList)
                {
                    var value = item.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(item);
                    string indexName = await _collectionNameProvider.RemoveCollectionPrefix(fullIndexNameList[indexNameCount]);
                    var nonShardKeyRouteIndexModel = new NonShardKeyRouteCollection()
                    {
                        Id = item.Id.ToString(),
                        ShardCollectionName = indexName,
                        // SearchKey = Convert.ChangeType(value, nonShardKey.FieldValueType)
                        SearchKey = value?.ToString()
                    };
                    nonShardKeyRouteBulk.Operations.Add(
                        new BulkIndexOperation<NonShardKeyRouteCollection>(nonShardKeyRouteIndexModel));
                    indexNameCount++;
                }

                var nonShardKeyRouteResponse = await client.BulkAsync(nonShardKeyRouteBulk, cancellationToken);
            }
        }
    }

    private async Task AddNonShardKeyRoute(TEntity model,string fullIndexName, IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (!_elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            return;
        }
        
        string indexName = await _collectionNameProvider.RemoveCollectionPrefix(fullIndexName);
        
        if (_nonShardKeyRouteProvider.NonShardKeys!=null && _nonShardKeyRouteProvider.NonShardKeys.Any())
        {
            foreach (var nonShardKey in _nonShardKeyRouteProvider.NonShardKeys)
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
        
        if (_nonShardKeyRouteProvider.NonShardKeys!=null && _nonShardKeyRouteProvider.NonShardKeys.Any())
        {
            foreach (var nonShardKey in _nonShardKeyRouteProvider.NonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteIndexId = model.Id.ToString();
                var nonShardKeyRouteIndexModel =
                    await _nonShardKeyRouteProvider.GetNonShardKeyRouteIndexAsync(nonShardKeyRouteIndexId,
                        nonShardKeyRouteIndexName);
                // var nonShardKeyRouteIndexModel = GetAsync((TKey)Convert.ChangeType(nonShardKeyRouteIndexId, typeof(TKey)), nonShardKeyRouteIndexName)  as NonShardKeyRouteCollection;

                var value = model.GetType().GetProperty(nonShardKey.FieldName)?.GetValue(model);
                if (nonShardKeyRouteIndexModel != null && nonShardKeyRouteIndexModel.SearchKey != value?.ToString())
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

    private async Task DeleteManyNonShardKeyRoute(List<TEntity> modelList,IElasticClient client,CancellationToken cancellationToken = default)
    {
        if (_nonShardKeyRouteProvider.NonShardKeys!=null && _nonShardKeyRouteProvider.NonShardKeys.Any() && _elasticIndexService.IsShardingCollection(typeof(TEntity)))
        {
            foreach (var nonShardKey in _nonShardKeyRouteProvider.NonShardKeys)
            {
                var nonShardKeyRouteIndexName =
                    _elasticIndexService.GetNonShardKeyRouteIndexName(typeof(TEntity), nonShardKey.FieldName);
                var nonShardKeyRouteBulk = new BulkRequest(nonShardKeyRouteIndexName)
                {
                    Operations = new List<IBulkOperation>(),
                    Refresh = _elasticsearchOptions.Refresh
                };
                foreach (var item in modelList)
                {
                    nonShardKeyRouteBulk.Operations.Add(new BulkDeleteOperation<NonShardKeyRouteCollection>(new Id(item)));
                }
                
                var nonShardKeyRouteResponse = await client.BulkAsync(nonShardKeyRouteBulk, cancellationToken);
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
        if (_nonShardKeyRouteProvider.NonShardKeys!=null && _nonShardKeyRouteProvider.NonShardKeys.Any())
        {
            foreach (var nonShardKey in _nonShardKeyRouteProvider.NonShardKeys)
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