using System.Collections.Generic;
using AElf.BaseStorageMapper;
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

        context.Services.Configure<IndexSettingOptions>(options => { options.Refresh = Refresh.True; });
        Configure<IndexCreateOption>(x =>
        {
            x.AddModule(typeof(AElfBaseStorageMapperTestsModule));
        });
        context.Services.Configure<EsEndpointOption>(options =>
        {
            options.Uris = new List<string> { "http://127.0.0.1:9200"};
        });
            
        context.Services.Configure<IndexSettingOptions>(options =>
        {
            options.NumberOfReplicas = 1;
            options.NumberOfShards = 6;
            options.Refresh = Refresh.True;
            options.IndexPrefix = "AElfIndexer";
        });
        
        context.Services.Configure<ShardInitSettingOptions>(options =>
        {
            options.ShardInitSettings = InitShardInitSettingOptions();
        });
        
        
    }

    private List<ShardInitSettingDto> InitShardInitSettingOptions()
    {
        ShardInitSettingDto blockIndexDto = new ShardInitSettingDto();
        blockIndexDto.IndexName = "BlockIndex";
        blockIndexDto.ShardChains = new List<ShardChain>()
        {
            new ShardChain()
            {
                ShardKeys = new List<ShardKey>()
                {
                    new ShardKey()
                    {
                        Name = "ChainId",
                        Value = "AELF",
                        Step = "",
                        StepType = 0
                    },
                    new ShardKey()
                    {
                        Name = "BlockHeight",
                        Value = "0",
                        Step = "2000",
                        StepType = 1
                    }
                }
            },
            new ShardChain()
            {
                ShardKeys = new List<ShardKey>()
                {
                    new ShardKey()
                    {
                        Name = "ChainId",
                        Value = "tDVV",
                        Step = "",
                        StepType = 0
                    },
                    new ShardKey()
                    {
                        Name = "BlockHeight",
                        Value = "0",
                        Step = "1000",
                        StepType = 1
                    }
                }
            }
        };
        
        ShardInitSettingDto logEventIndexDto = new ShardInitSettingDto();
        logEventIndexDto.IndexName = "LogEventIndex";
        logEventIndexDto.ShardChains = new List<ShardChain>()
        {
            new ShardChain()
            {
                ShardKeys = new List<ShardKey>()
                {
                    new ShardKey()
                    {
                        Name = "ChainId",
                        Value = "AELF",
                        Step = "",
                        StepType = 0
                    },
                    new ShardKey()
                    {
                        Name = "BlockHeight",
                        Value = "0",
                        Step = "2000",
                        StepType = 1
                    }
                }
            },
            new ShardChain()
            {
                ShardKeys = new List<ShardKey>()
                {
                    new ShardKey()
                    {
                        Name = "ChainId",
                        Value = "tDVV",
                        Step = "",
                        StepType = 0
                    },
                    new ShardKey()
                    {
                        Name = "BlockHeight",
                        Value = "0",
                        Step = "1000",
                        StepType = 1
                    }
                }
            }
        };
        
        return new List<ShardInitSettingDto>()
        {
            blockIndexDto,
            logEventIndexDto
        };
        
    }

}