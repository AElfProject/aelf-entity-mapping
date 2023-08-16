namespace AElf.EntityMapping.Elasticsearch;

public static class IndexNameHelper
{
    public static string FormatIndexName(List<string> indices)
    {
        return indices == null ? string.Empty : string.Join(',', indices);
    }
    
    public static string RemoveCollectionPrefix(string fullCollectionName,string collectionPrefix)
    {
        var collectionName = fullCollectionName;
        if (!string.IsNullOrWhiteSpace(collectionPrefix))
        {
            collectionName = collectionName.RemovePreFix($"{collectionPrefix}.".ToLower());
        }

        return collectionName;
    }
    public static string GetDefaultIndexName(Type type)
    {
        // TODO: Maybe not correct? If not sharded there will be no prefix.
        // var indexName = _entityMappingOptions.CollectionPrefix.IsNullOrWhiteSpace()
        //     ? type.Name.ToLower()
        //     : $"{_entityMappingOptions.CollectionPrefix.ToLower()}.{type.Name.ToLower()}";
        return type.Name.ToLower();
    }
    //TODO: Need to be the same algorithm as the CollectionNameProviderBase.GetFullCollectionNameAsync
    public static string GetDefaultFullIndexName(Type type,string collectionPrefix)
    {
        var fullIndexName=collectionPrefix.IsNullOrWhiteSpace()
            ? GetDefaultIndexName(type)
            : $"{collectionPrefix.ToLower()}.{GetDefaultIndexName(type)}";
        return fullIndexName;
    }
    public static string GetNonShardKeyRouteIndexName(Type type, string fieldName,string collectionPrefix)
    {
        var routeIndexName= collectionPrefix.IsNullOrWhiteSpace()
            ? $"route.{type.Name.ToLower()}.{fieldName.ToLower()}"
            : $"{collectionPrefix.ToLower()}.route.{type.Name.ToLower()}.{fieldName.ToLower()}";
        return routeIndexName;
    }
}