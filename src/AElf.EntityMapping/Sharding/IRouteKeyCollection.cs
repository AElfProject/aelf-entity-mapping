namespace AElf.EntityMapping.Sharding;

public interface IRouteKeyCollection
{
    string Id { get; set; }

    string CollectionName { get; set; }
    //can only support string type
    string CollectionRouteKey { get; set; }
}