namespace AElf.EntityMapping.Elasticsearch;

public static class IndexNameHelper
{
    public static string FormatIndexName(List<string> indices)
    {
        return indices == null ? string.Empty : string.Join(',', indices);
    }
}