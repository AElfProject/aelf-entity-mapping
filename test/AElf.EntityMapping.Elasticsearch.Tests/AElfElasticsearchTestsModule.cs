using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.TestBase;
using Elasticsearch.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Volo.Abp;
using Volo.Abp.Modularity;

namespace AElf.EntityMapping.Elasticsearch;

[DependsOn(typeof(AElfEntityMappingElasticsearchModule),
    typeof(AElfEntityMappingTestBaseModule),
    typeof(AElfEntityMappingTestModule)
)]
public class AElfElasticsearchTestsModule : AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
        Configure<CollectionCreateOptions>(x =>
        {
            x.AddModule(typeof(AElfElasticsearchTestsModule));
        });
        
        Configure<ElasticsearchOptions>(options =>
        {
            options.Refresh = Refresh.True;
        });
    }

    public override void OnApplicationShutdown(ApplicationShutdownContext context)
    {
        var option = context.ServiceProvider.GetRequiredService<IOptionsSnapshot<AElfEntityMappingOptions>>();
        if(option.Value.CollectionPrefix.IsNullOrEmpty())
            return;
        
        var clientProvider = context.ServiceProvider.GetRequiredService<IElasticsearchClientProvider>();
        var client = clientProvider.GetClient();
        var indexPrefix = option.Value.CollectionPrefix.ToLower();
        
        client.Indices.Delete(indexPrefix+"*");
        client.Indices.DeleteTemplate(indexPrefix + "*");
    }
}