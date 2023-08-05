using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Options;
using AElf.EntityMapping.TestBase;
using Elasticsearch.Net;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp;
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
            x.AddModule(typeof(AElfEntityMappingTestModule));
        });
        
        Configure<ElasticsearchOptions>(options =>
        {
            options.Refresh = Refresh.True;
        });
    }

    public override void OnApplicationShutdown(ApplicationShutdownContext context)
    {
        var clientProvider = context.ServiceProvider.GetRequiredService<IElasticsearchClientProvider>();
        var indexNameProvider = context.ServiceProvider.GetRequiredService<IIndexNameProvider>();

        var client = clientProvider.GetClient();
        var indexNames = indexNameProvider.GetIndexNames();
        foreach (var indexName in indexNames)
        {
            client.Indices.Delete(indexName);
        }
        indexNameProvider.ClearIndexName();
        
        var indexTemplateNames = indexNameProvider.GetIndexTemplates();
        foreach (var indexTemplateName in indexTemplateNames)
        {
            client.Indices.Delete(indexTemplateName.TrimStart('.').Replace("template", "*"));
            client.Indices.DeleteTemplate(indexTemplateName);
        }
        indexNameProvider.ClearIndexTemplate();
    }
}