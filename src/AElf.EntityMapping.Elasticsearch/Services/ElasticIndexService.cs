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
    private readonly List<ShardInitSetting> _indexSettingDtos;
    
    public ElasticIndexService(IElasticsearchClientProvider elasticsearchClientProvider,
        ILogger<ElasticIndexService> logger,
        IOptions<AElfEntityMappingOptions> entityMappingOptions,
        IOptions<ElasticsearchOptions> elasticsearchOptions)
        {
        _elasticsearchClientProvider = elasticsearchClientProvider;
        _logger = logger;
        _entityMappingOptions = entityMappingOptions.Value;
        _elasticsearchOptions = elasticsearchOptions.Value;
        _indexSettingDtos = entityMappingOptions.Value.ShardInitSettings;
    }

    public Task<IElasticClient> GetElasticsearchClientAsync()
    {
        return Task.FromResult(_elasticsearchClientProvider.GetClient());
    }
    
    public async Task CreateIndexAsync(string indexName, Type type, int shard = 1, int numberOfReplicas = 1, Dictionary<string, object> indexSettings = null)
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
                            o =>
                            {
                                var setting =  o.NumberOfShards(shard).NumberOfReplicas(numberOfReplicas)
                                    .Setting("max_result_window", _elasticsearchOptions.MaxResultWindow);
                                if (indexSettings != null)
                                {
                                    foreach (var indexSetting in indexSettings)
                                    {
                                        setting.Setting(indexSetting.Key, indexSetting.Value);
                                    }
                                }
                                return setting;
                            })
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

    public async Task CreateCollectionRouteKeyIndexAsync(Type type, int shard = 1, int numberOfReplicas = 1)
    {
        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        foreach (var property in properties)
        {
            CollectionRouteKeyAttribute shardRouteAttribute = (CollectionRouteKeyAttribute)Attribute.GetCustomAttribute(property, typeof(CollectionRouteKeyAttribute));
            if (shardRouteAttribute != null)
            {
                if (property.PropertyType != typeof(string))
                {
                    throw new NotSupportedException(
                        $"{type.Name} Attribute Error! NeedShardRouteAttribute only support string type, please check field: {property.Name}");
                }
                var indexName = IndexNameHelper.GetCollectionRouteKeyIndexName(type, property.Name,_entityMappingOptions.CollectionPrefix);
                await CreateIndexAsync(indexName, typeof(RouteKeyCollection), shard, numberOfReplicas);
            }
        }
    }
    
    public async Task DeleteIndexAsync(string collectionName = null, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(collectionName))
        {
            throw new ArgumentNullException(nameof(collectionName), "Collection name must be provided.");
        }

        try
        {
            var elasticClient = await GetElasticsearchClientAsync();
            var response = await elasticClient.Indices.DeleteAsync(collectionName, ct: cancellationToken);
            if (!response.IsValid)
            {
                if (response.ServerError == null)
                {
                    return;
                }
                
                if (response.ServerError?.Status == 404)
                {
                    _logger.LogError("Failed to delete index {0} does not exist.", collectionName);
                    return;
                }

                // Log the error or throw an exception based on the response
                throw new ElasticsearchException($"Failed to delete index {collectionName}: {response.ServerError.Error.Reason}");
            }

            _logger.LogInformation("Index {0} deleted successfully.", collectionName);
        }
        catch (Exception ex)
        {
            // Handle exceptions from the client (network issues, etc.)
            throw new ElasticsearchException($"An error occurred while delete index {collectionName}: {ex.Message}");
        }
    }
}