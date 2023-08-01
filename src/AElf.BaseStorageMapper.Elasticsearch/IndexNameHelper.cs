namespace AElf.BaseStorageMapper.Elasticsearch;

public static class IndexNameHelper
{
    public static string FormatIndexName(List<string> indices)
    {
        return string.Join(',', indices);
    }
}