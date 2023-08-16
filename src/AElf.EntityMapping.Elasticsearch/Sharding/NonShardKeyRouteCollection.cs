using AElf.EntityMapping.Entities;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class NonShardKeyRouteCollection:Entity,IEntity<string>,IEntityMappingEntity
{
    [Keyword]public string Id { get; set; }

    [Keyword]public string CollectionName { get; set; }
    //can only support string type
    [Keyword]public string CollectionRouteKey { get; set; }
    
    public override object[] GetKeys()
    {
        return new object[] {Id};
    }
}