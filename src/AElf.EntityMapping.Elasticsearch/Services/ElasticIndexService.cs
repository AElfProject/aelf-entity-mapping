using System.Reflection;
using AElf.EntityMapping.Elasticsearch.Exceptions;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Elasticsearch.Sharding;
using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Nest;
using Volo.Abp.Caching;
using Volo.Abp.DependencyInjection;

namespace AElf.EntityMapping.Elasticsearch.Services;

public class ElasticIndexService: IElasticIndexService, ITransientDependency
{
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ElasticIndexService> _logger;
    private readonly AElfEntityMappingOptions _entityMappingOptions;
    private readonly ElasticsearchOptions _elasticsearchOptions;
    // private readonly IDistributedCache<List<CollectionRouteKeyItem>> _collectionRouteKeyCache;
    // private readonly string _indexMarkFieldCachePrefix = "MarkField_";
    // private const string CollectionRouteKeyCacheNameFormat = "RouteKeyCache_{0}";
    private readonly List<ShardInitSetting> _indexSettingDtos;
    
    public ElasticIndexService(IElasticsearchClientProvider elasticsearchClientProvider,
        ILogger<ElasticIndexService> logger, 
        // IDistributedCache<List<CollectionRouteKeyItem>> collectionRouteKeyCache,
        IOptions<AElfEntityMappingOptions> entityMappingOptions,
        IOptions<ElasticsearchOptions> elasticsearchOptions)
        {
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _logger = logger;
        _entityMappingOptions = entityMappingOptions.Value;
        _elasticsearchOptions = elasticsearchOptions.Value;
        // _collectionRouteKeyCache = collectionRouteKeyCache;
        //_indexShardOptions = indexShardOptions.Value;
        _indexSettingDtos = entityMappingOptions.Value.ShardInitSettings;
    }

    public Task<IElasticClient> GetElasticsearchClientAsync()
    {
        return Task.FromResult(_elasticsearchClientProvider.GetClient());
    }
    
    public async Task CreateIndexAsync(string indexName, Type type, int shard = 1, int numberOfReplicas = 1)
    {
        if (!type.IsClass || type.IsAbstract || !typeof(IEntityMappingEntity).IsAssignableFrom(type))
        {
            _logger.LogInformation($" type: {type.FullName} invalid type");
            return;
        }
        var client = await GetElasticsearchClientAsync();
        var exits = await client.Indices.ExistsAsync(indexName);
            
        if (exits.Exists)
        {
            _logger.LogInformation($" index: {indexName} type: {type.FullName} existed");
            return;
        }
        _logger.LogInformation($"create index for type {type.FullName}  index name: {indexName}");
        //var newName = indexName + DateTime.Now.Ticks;
        var result = await client
            .Indices.CreateAsync(indexName,
                ss =>
                    ss.Index(indexName)
                        .Settings(
                            o => o.NumberOfShards(shard).NumberOfReplicas(numberOfReplicas)
                                .Setting("max_result_window", _elasticsearchOptions.MaxResultWindow))
                        .Map(m => m.AutoMap(type)));
        if (!result.Acknowledged)
            throw new ElasticsearchException($"Create Index {indexName} failed : " +
                                             result.ServerError.Error.Reason);
        
        //await client.Indices.PutAliasAsync(newName, indexName);
    }

    public async Task CreateIndexTemplateAsync(string indexTemplateName,string indexName, Type type, int numberOfShards = 1,
        int numberOfReplicas = 1)
    {
        var elasticClient = await GetElasticsearchClientAsync();
        
        // Check if the index template already exists
        var templateExistsResponse = elasticClient.Indices.GetTemplate(new GetIndexTemplateRequest(indexTemplateName));
        if (templateExistsResponse.TemplateMappings.Count > 0)
        {
            _logger.LogInformation("Index template {indexTemplateName} already exists", indexTemplateName);
            return;
        }

        // Add an index template to Elasticsearch
        var putIndexTemplateResponse = elasticClient.Indices.PutTemplate(indexTemplateName, p => p
            .IndexPatterns(indexName + "*")
            .Mappings(m => m
                .Map(t => t.AutoMap(type))
            )
            .Settings(s => s
                    .NumberOfShards(numberOfShards)
                    .NumberOfReplicas(numberOfReplicas)
                // .Analysis(a => a
                //     .Analyzers(an => an
                //         .Custom("my_custom_analyzer", ca => ca
                //             .Tokenizer("standard")
                //             .Filters("lowercase", "stop")
                //         )
                //     )
                // )
            ).Aliases(a=>a.Alias(indexName))
        );

        // Check the creation status of the index template
        if (!putIndexTemplateResponse.IsValid)
        {
            var errorMessage = putIndexTemplateResponse.OriginalException.Message;
            _logger.LogError("Failed to create index template {indexTemplateName}: {errorMessage}", indexTemplateName,
                errorMessage);
            throw new ElasticsearchException(errorMessage);
        }
        else
        {
            _logger.LogInformation("Index template {indexTemplateName} created successfully", indexTemplateName);
        }
    }

    // public async Task InitializeCollectionRouteKeyCacheAsync(Type type)
    // {
    //     var collectionRouteKeyCacheItems = new List<CollectionRouteKeyCacheItem>();
    //     var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
    //     foreach (var property in properties)
    //     {
    //         var indexMarkField = new CollectionRouteKeyCacheItem
    //         {
    //             FieldName = property.Name,
    //             // FieldValueType = property.PropertyType.ToString(),
    //             CollectionName = type.Name,
    //         };
    //         
    //         //Find the field with the ShardPropertyAttributes annotation set
    //         // ShardPropertyAttributes shardAttribute = (ShardPropertyAttributes)Attribute.GetCustomAttribute(property, typeof(ShardPropertyAttributes));
    //         // if (shardAttribute != null)
    //         // {
    //         //     indexMarkField.IsShardKey = true;
    //         // }
    //
    //         //Find the field with the ShardRoutePropertyAttributes annotation set
    //         CollectionRoutekeyAttribute shardRouteAttribute = (CollectionRoutekeyAttribute)Attribute.GetCustomAttribute(property, typeof(CollectionRoutekeyAttribute));
    //         if (shardRouteAttribute != null)
    //         {
    //             // indexMarkField.IsRouteKey = true;
    //             collectionRouteKeyCacheItems.Add(indexMarkField);
    //         }
    //         
    //         // indexMarkFieldList.Add(indexMarkField);
    //     }
    //
    //     var cacheName = GetCollectionRouteKeyCacheName(type);
    //     await _collectionRouteKeyCache.SetAsync(cacheName, collectionRouteKeyCacheItems, hideErrors: false);
    //     
    //     _logger.LogInformation("{cacheName} cached successfully", cacheName);
    // }

    public async Task CreateNonShardKeyRouteIndexAsync(Type type, int shard = 1, int numberOfReplicas = 1)
    {
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        foreach (var property in properties)
        {
            CollectionRoutekeyAttribute shardRouteAttribute = (CollectionRoutekeyAttribute)Attribute.GetCustomAttribute(property, typeof(CollectionRoutekeyAttribute));
            if (shardRouteAttribute != null)
            {
                if (property.PropertyType != typeof(string))
                {
                    throw new NotSupportedException(
                        $"{type.Name} Attribute Error! NeedShardRouteAttribute only support string type, please check field: {property.Name}");
                }
                var indexName = IndexNameHelper.GetNonShardKeyRouteIndexName(type, property.Name,_entityMappingOptions.CollectionPrefix);
                await CreateIndexAsync(indexName, typeof(NonShardKeyRouteCollection), shard, numberOfReplicas);
            }
        }
    }

    // public string GetCollectionRouteKeyCacheName(Type type)
    // {
    //     // var cacheName = _entityMappingOptions.CollectionPrefix.IsNullOrWhiteSpace()
    //     //     ? $"{_indexMarkFieldCachePrefix}_{type.FullName}"
    //     //     : $"{_indexMarkFieldCachePrefix}{_entityMappingOptions.CollectionPrefix}_{type.FullName}";
    //     var cacheName = string.Format(CollectionRouteKeyCacheNameFormat,
    //         _entityMappingOptions.CollectionPrefix.IsNullOrWhiteSpace()
    //             ? type.FullName
    //             : $"{_entityMappingOptions.CollectionPrefix}_{type.FullName}");
    //     return cacheName;
    // }

    
}