using Microsoft.Extensions.DependencyInjection;
using Volo.Abp.Modularity;

namespace AElf.BaseStorageMapper
{
    public class AElfBaseStorageMapperModule : AbpModule
    {
        public override void ConfigureServices(ServiceConfigurationContext context)
        {
            var configuration = context.Services.GetConfiguration();
            Configure<AElfBaseStorageMapperOptions>(configuration.GetSection("BaseStorageMapper"));
            
            var services = context.Services;
            services.AddTransient(typeof(ICollectionNameProvider<>), typeof(DefaultCollectionNameProvider<>));
        }
    }
}