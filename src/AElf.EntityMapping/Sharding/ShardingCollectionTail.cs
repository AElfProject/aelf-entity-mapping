using AElf.EntityMapping.Entities;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Sharding;

public class ShardingCollectionTail:Entity,IEntity<string>,IEntityMappingEntity
{
    [Keyword]public string Id { get; set; }
    
    [Keyword]public string EntityName { get; set; }

    [Keyword]public string TailPrefix { get; set; }

    public long Tail { get; set; }
    
    public override object[] GetKeys()
    {
        return new object[] {Id};
    }
}