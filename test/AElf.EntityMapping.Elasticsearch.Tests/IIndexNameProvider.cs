using System.Collections.Concurrent;
using Volo.Abp.DependencyInjection;

namespace AElf.EntityMapping.Elasticsearch;

public interface IIndexNameProvider
{
    void AddIndexName(string indexName);
    void AddIndexTemplate(string indexTemplate);
    List<string> GetIndexNames();
    List<string> GetIndexTemplates();
    void ClearIndexName();
    void ClearIndexTemplate();
}

public class IndexNameProvider : IIndexNameProvider, ISingletonDependency
{
    private ConcurrentBag<string> IndexNames { get; set; } = new();
    private ConcurrentBag<string> IndexTemplates { get; set; } = new();

    public void AddIndexName(string indexName)
    {
        IndexNames.Add(indexName);
    }

    public void AddIndexTemplate(string indexTemplate)
    {
        IndexTemplates.Add(indexTemplate);
    }

    public List<string> GetIndexNames()
    {
        return IndexNames.ToList();
    }

    public List<string> GetIndexTemplates()
    {
        return IndexTemplates.ToList();
    }

    public void ClearIndexName()
    {
        IndexNames.Clear();
    }

    public void ClearIndexTemplate()
    {
        IndexTemplates.Clear();
    }
}