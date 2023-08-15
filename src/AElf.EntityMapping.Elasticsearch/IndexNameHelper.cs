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
}