using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.TestBase;
using Elasticsearch.Net;
using Volo.Abp.Modularity;

namespace AElf.EntityMapping.Elasticsearch;

[DependsOn(typeof(AElfEntityMappingElasticsearchModule),
    typeof(AElfEntityMappingTestBaseModule)
)]
public class AElfElasticsearchTestsModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        Configure<CollectionCreateOption>(x =>
        {
            x.AddModule(typeof(AElfElasticsearchTestsModule));
        });
        
        Configure<ElasticsearchOptions>(options =>
        {
            options.Refresh = Refresh.True;
        });
    }
}