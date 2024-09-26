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
        return type.Name.ToLower();
    }
    //Attention: this need to be the same algorithm as the CollectionNameProviderBase.GetFullCollectionNameAsync
    public static string GetDefaultFullIndexName(Type type,string collectionPrefix)
    {
        var fullIndexName=collectionPrefix.IsNullOrWhiteSpace()
            ? GetDefaultIndexName(type)
            : $"{collectionPrefix.ToLower()}.{GetDefaultIndexName(type)}";
        return fullIndexName;
    }
}