using Nest;

namespace AElf.EntityMapping.Elasticsearch;

public class ElasticsearchResponseHelper
{
    public static string GetErrorMessage(IResponse response)
    {
        if (response.ServerError == null)
        {
            if (response.OriginalException == null)
            {
                return "Unknown error.";
            }

            if (response.OriginalException.InnerException == null)
            {
                return response.OriginalException.Message;
            }
            
            return response.OriginalException.InnerException.Message;
        }

        return response.ServerError.ToString();
    }
}