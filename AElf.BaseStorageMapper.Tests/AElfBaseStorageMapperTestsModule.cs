using System.Collections.Generic;
using AElf.BaseStorageMapper;
using AElf.BaseStorageMapper.Elasticsearch.Options;
using AElf.BaseStorageMapper.Options;
using AElf.BaseStorageMapper.TestBase;
/*using AElf.Indexing.Elasticsearch;
using AElf.Indexing.Elasticsearch.Options;
using AElfLinqToElasticSearchTestBase;*/
using Elasticsearch.Net;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp.AutoMapper;
using Volo.Abp.Modularity;

namespace AElf.LinqToElasticSearch;

[DependsOn(typeof(AElfBaseStorageMapperModule),
    typeof(AElfBaseStorageMapperTestBaseModule)
   //, typeof(AElfIndexingElasticsearchModule)
)]
public class AElfBaseStorageMapperTestsModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        Configure<AbpAutoMapperOptions>(options => { options.AddMaps<AElfBaseStorageMapperTestsModule>(); });

        context.Services.Configure<ElasticsearchOptions>(options => { options.Refresh = Refresh.True; });
        Configure<CollectionCreateOption>(x =>
        {
            x.AddModule(typeof(AElfBaseStorageMapperTestsModule));
        });
        context.Services.Configure<EsEndpointOption>(options =>
        {
            options.Uris = new List<string> { "http://127.0.0.1:9200"};
        });
            
        context.Services.Configure<ElasticsearchOptions>(options =>
        {
            options.NumberOfReplicas = 1;
            options.NumberOfShards = 6;
            options.Refresh = Refresh.True;
            // options.IndexPrefix = "AElfIndexer";
        });
        
        Configure<AElfBaseStorageMapperOptions>(options =>
        {
            options.CollectionPrefix = "AElfIndexer";
        });

    }

    

}