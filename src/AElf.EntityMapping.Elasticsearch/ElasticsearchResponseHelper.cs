using Nest;

namespace AElf.EntityMapping.Elasticsearch;

public class ElasticsearchResponseHelper
{
    public static string GetErrorMessage(IResponse response)
    {
        return response.ServerError == null ? "Unknown error." : response.ServerError.ToString();
    }
}