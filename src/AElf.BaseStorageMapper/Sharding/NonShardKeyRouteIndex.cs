using Nest;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Sharding;

public class NonShardKeyRouteIndex:Entity,IEntity<string>
{
    [Keyword]public string Id { get; set; }

    [Keyword]public string ShardCollectionName { get; set; }
    //can only support string type
    [Keyword]public string SearchKey { get; set; }
    
    public override object[] GetKeys()
    {
        return new object[] {Id};
    }
}