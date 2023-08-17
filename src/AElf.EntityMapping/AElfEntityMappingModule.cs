using AElf.EntityMapping.Options;
using AElf.EntityMapping.Sharding;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
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
        }
        
        public override void OnPreApplicationInitialization(ApplicationInitializationContext context)
        { 
            CheckAElfEntityMappingOptions(context);
        }

        private void CheckAElfEntityMappingOptions(ApplicationInitializationContext context)
        {
            try
            {
                var option = context.ServiceProvider.GetRequiredService<IOptionsSnapshot<AElfEntityMappingOptions>>();
                if (option.Value == null)
                    throw new Exception("AElfEntityMappingOptions config cant be null");
                
                var shardInitSettings =  option.Value.ShardInitSettings;
                if (shardInitSettings.IsNullOrEmpty())
                    return ;

                foreach (var shardInitSetting in shardInitSettings)
                {
                    var shardGroups = shardInitSetting.ShardGroups;
                    if (shardGroups == null || shardGroups.Count == 0)
                    {
                        throw new Exception("AElfEntityMappingOptions.ShardGroups config cant be null");
                    }
                    
                    Dictionary<string,string> shardKeyDic = new Dictionary<string, string>();
                    foreach (var shardGroup in shardGroups)
                    {
                        var shardKeys = shardGroup.ShardKeys;
                        if (shardKeys == null || shardKeys.Count == 0)
                        {
                            throw new Exception("AElfEntityMappingOptions.ShardGroups.ShardKeys config cant be null");
                        }

                        foreach (var shardKey in shardKeys)
                        {
                            if (shardKey.StepType == StepType.Floor)
                            {
                                if (int.TryParse(shardKey.Step, out var step))
                                {
                                    if (step <= 0)
                                    {
                                        throw new Exception($"AElfEntityMappingOptions.ShardGroups.ShardKeys.Step config is not correct,  StepType.Floor Step:{step} must be greater than 0");
                                    }
                                }
                                else
                                {
                                    throw new Exception($"AElfEntityMappingOptions.ShardGroups.ShardKeys.Step config is not correct,  StepType.Floor Step:{shardKey.Step} must be int");
                                }
                                continue;
                            }

                            if (!shardKeyDic.TryGetValue(shardKey.Name, out var value))
                            {
                                shardKeyDic.Add(shardKey.Name, shardKey.Value);
                            }
                            else
                            {
                                if (value == shardKey.Value)
                                {
                                    throw new Exception($"AElfEntityMappingOptions.ShardGroups.ShardKeys.Value config is not correct,  StepType.None Value:{value} must be not consistent");
                                }
                            }
                        }
                        
                    }
                }
                
            }catch(Exception e)
            {
                throw new Exception("CheckAElfEntityMappingOptions Exception", e);
            }
        }
    }
}