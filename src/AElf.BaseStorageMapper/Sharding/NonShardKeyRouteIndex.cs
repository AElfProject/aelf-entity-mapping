namespace AElf.BaseStorageMapper.Sharding;

public class NonShardKeyRouteIndex
{
    public string Id { get; set; }

    public string ShardCollectionName { get; set; }
    public object SearchKey { get; set; }
}