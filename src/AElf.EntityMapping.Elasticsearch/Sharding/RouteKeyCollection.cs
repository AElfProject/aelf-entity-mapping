using AElf.EntityMapping.Entities;
using AElf.EntityMapping.Sharding;
using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.EntityMapping.Elasticsearch.Sharding;

public class RouteKeyCollection:Entity,IEntity<string>,IEntityMappingEntity,IRouteKeyCollection
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