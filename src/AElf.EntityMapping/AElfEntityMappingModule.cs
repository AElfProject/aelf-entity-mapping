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

        public void CheckAElfEntityMappingOptions(ApplicationInitializationContext context)
        {
            try
            {
                var option = context.ServiceProvider.GetRequiredService<IOptionsSnapshot<AElfEntityMappingOptions>>();
                if (option.Value == null)
                    throw new Exception("AElfEntityMappingOptions config cant be null");
                
                var shardInitSettingDtos =  option.Value.ShardInitSettings;
                if (shardInitSettingDtos.IsNullOrEmpty())
                    return ;

                foreach (var shardInitSettingDto in shardInitSettingDtos)
                {
                    var shardGroups = shardInitSettingDto.ShardGroups;
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
                        var checkStepValue = !shardKeys.Last().Value.IsNullOrWhiteSpace() && (shardKeys.Last().StepType == StepType.Floor);
                        if (!checkStepValue)
                        {
                            throw new Exception("AElfEntityMappingOptions.ShardGroups.ShardKeys.Step config is not correct, if step>0 need config at the end");
                        }
                        
                        var similarShardKeys = shardKeys.FindAll(a=>!a.Value.IsNullOrWhiteSpace() && (int.TryParse(a.Step, out var stepValue) && stepValue > 0));
                        if(similarShardKeys.Count == 0)
                            throw new Exception("AElfEntityMappingOptions.ShardGroups.ShardKeys.Step config is not correct,  step must greater than 0");
                        
                        if(similarShardKeys.Count > 1)
                            throw new Exception("AElfEntityMappingOptions.ShardGroups.ShardKeys.Step config is not correct, greater than 0 step config can only one");

                        foreach (var shardKey in shardKeys)
                        {
                            if (shardKey.StepType == StepType.Floor) continue;
                            
                            if (!shardKeyDic.TryGetValue(shardKey.Name, out var value))
                            {
                                shardKeyDic.Add(shardKey.Name, shardKey.Value);
                            }
                            else
                            {
                                if (value == shardKey.Value)
                                {
                                    throw new Exception($"AElfEntityMappingOptions.ShardGroups.ShardKeys.Value config is not correct,  Value:{value} must be not consistent");
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