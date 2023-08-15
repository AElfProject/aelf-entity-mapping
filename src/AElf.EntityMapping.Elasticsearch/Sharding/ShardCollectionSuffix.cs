using AElf.EntityMapping.Entities;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class ShardCollectionSuffix:Entity,IEntity<string>,IEntityMappingEntity
{
    [Keyword]public string Id { get; set; }
    
    [Keyword]public string EntityName { get; set; }

    [Keyword]public string Keys { get; set; }

    [Keyword]public long MaxShardNo { get; set; }
    
    public override object[] GetKeys()
    {
        return new object[] {Id};
    }
}