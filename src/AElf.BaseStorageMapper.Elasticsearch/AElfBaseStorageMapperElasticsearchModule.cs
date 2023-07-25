using Volo.Abp.Modularity;

namespace AElf.BaseStorageMapper.Elasticsearch;

[DependsOn(
    typeof(AElfBaseStorageMapperModule)
)]
public class AElfBaseStorageMapperElasticsearchModule: AbpModule
{
    public override void ConfigureServices(ServiceConfigurationContext context)
    {
            
    }
}