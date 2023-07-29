using System.Reflection;
using AElf.BaseStorageMapper.Elasticsearch.Exceptions;
using AElf.BaseStorageMapper.Sharding;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;
using Nest;
using Volo.Abp.Caching;
using Volo.Abp.DependencyInjection;

namespace AElf.BaseStorageMapper.Elasticsearch.Services;

public class ElasticIndexService: IElasticIndexService, ITransientDependency
{
    private readonly IElasticsearchClientProvider _elasticsearchClientProvider;
    private readonly ILogger<ElasticIndexService> _logger;
    private readonly IDistributedCache<List<IndexMarkField>> _indexMarkFieldCache;
    private readonly string _indexMarkFieldCachePrefix = "MarkField_";

    public ElasticIndexService(IElasticsearchClientProvider elasticsearchClientProvider, ILogger<ElasticIndexService> logger,IDistributedCache<List<IndexMarkField>> indexMarkFieldCache)
    {
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _logger = logger;
        _indexMarkFieldCache = indexMarkFieldCache;
    }

    public Task<IElasticClient> GetElasticsearchClientAsync()
    {
        return Task.FromResult(_elasticsearchClientProvider.GetClient());
    }
    
    public async Task CreateIndexAsync(string indexName, Type type, int shard = 1, int numberOfReplicas = 1)
    {
        if (!type.IsClass || type.IsAbstract || !typeof(IIndexBuild).IsAssignableFrom(type))
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
                                .Setting("max_result_window", int.MaxValue))
                        .Map(m => m.AutoMap(type)));
        if (!result.Acknowledged)
            throw new ElasticsearchException($"Create Index {indexName} failed : :" +
                                             result.ServerError.Error.Reason);
        //await client.Indices.PutAliasAsync(newName, indexName);
    }

    public async Task CreateIndexTemplateAsync(string indexTemplateName, Type type, int numberOfShards = 1,
        int numberOfReplicas = 1)
    {
        var elasticClient = await GetElasticsearchClientAsync();

        // Add an index template to Elasticsearch
        var putIndexTemplateResponse = elasticClient.Indices.PutTemplate(indexTemplateName, p => p
            .IndexPatterns(indexTemplateName + "*")
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
            )
        );

        // Check the creation status of the index template
        if (!putIndexTemplateResponse.IsValid)
        {
            var errorMessage = putIndexTemplateResponse.OriginalException.Message;
            _logger.LogError($"Failed to create index template: {errorMessage}");
        }
        else
        {
            _logger.LogInformation("Index template created successfully");
        }
    }

    public async Task InitializeIndexMarkedFieldAsync(Type type)
    {
        var indexMarkFieldList = new List<IndexMarkField>();
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        foreach (var property in properties)
        {
            var indexMarkField = new IndexMarkField
            {
                FieldName = property.Name,
                IndexEntityName = type.Name,
            };
            
            //Find the field with the ShardPropertyAttributes annotation set
            ShardPropertyAttributes shardAttribute = (ShardPropertyAttributes)Attribute.GetCustomAttribute(property, typeof(ShardPropertyAttributes));
            if (shardAttribute != null)
            {
                indexMarkField.IsShardKey = true;
            }

            //Find the field with the ShardRoutePropertyAttributes annotation set
            NeedShardRouteAttribute shardRouteAttribute = (NeedShardRouteAttribute)Attribute.GetCustomAttribute(property, typeof(NeedShardRouteAttribute));
            if (shardRouteAttribute != null)
            {
                indexMarkField.IsRouteKey = true;
            }
            
            indexMarkFieldList.Add(indexMarkField);
        }

        var cacheName = await GetIndexMarkFieldNameAsync(type);
        _indexMarkFieldCache.Set(cacheName, indexMarkFieldList);
    }

    public Task<string> GetIndexMarkFieldNameAsync(Type type)
    {
        return Task.FromResult(_indexMarkFieldCachePrefix + type.Name);
    }
}