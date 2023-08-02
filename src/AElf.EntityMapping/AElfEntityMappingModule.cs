using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.DependencyInjection;
using Volo.Abp;
using Volo.Abp.Caching;
using Volo.Abp.Modularity;

namespace AElf.EntityMapping
{
    
    [DependsOn(
        typeof(AbpCachingModule)
    )]
    public class AElfEntityMappingModule : AbpModule
    {
        public override void ConfigureServices(ServiceConfigurationContext context)
        {
            var configuration = context.Services.GetConfiguration();
            Configure<AElfEntityMappingOptions>(configuration.GetSection("AElfEntityMapping"));
            
            var services = context.Services;
            services.AddTransient(typeof(ICollectionNameProvider<>), typeof(DefaultCollectionNameProvider<>));
            
            // Configure<IndexSettingOptions>(configuration.GetSection("IndexSetting"));
            Configure<ShardInitSettingOptions>(configuration.GetSection("ShardSetting"));
        }
        
        public override void OnPreApplicationInitialization(ApplicationInitializationContext context)
        {
            
        }
        
    }
}