using AElf.EntityMapping.Elasticsearch;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.TestBase;
using Elasticsearch.Net;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp.AutoMapper;
using Volo.Abp.Modularity;

namespace AElf.EntityMapping;

[DependsOn(typeof(AElfEntityMappingModule),
    typeof(AElfEntityMappingTestBaseModule)
    , typeof(AElfEntityMappingElasticsearchModule)
)]
public class AElfEntityMappingTestModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        Configure<AbpAutoMapperOptions>(options => { options.AddMaps<AElfEntityMappingTestModule>(); });
        
        Configure<CollectionCreateOption>(x =>
        {
            x.AddModule(typeof(AElfEntityMappingTestModule));
        });
        
        Configure<AElfEntityMappingOptions>(options =>
        {
            options.CollectionPrefix = "AElfEntityMappingTest";
        });

        // TODO: move to AElf.EntityMapping.Elasticsearch.Tests
        context.Services.Configure<ElasticsearchOptions>(options =>
        {
            options.NumberOfReplicas = 1;
            options.NumberOfShards = 6;
            options.Refresh = Refresh.True;
            // options.IndexPrefix = "AElfIndexer";
        });

    }
}