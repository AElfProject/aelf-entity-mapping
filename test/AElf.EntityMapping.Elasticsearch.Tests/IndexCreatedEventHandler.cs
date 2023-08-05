using AElf.EntityMapping.Elasticsearch.Services;
using Volo.Abp.DependencyInjection;
using Volo.Abp.EventBus;

namespace AElf.EntityMapping.Elasticsearch;

public class IndexCreatedEventHandler : ILocalEventHandler<IndexCreatedEventData>,
    ILocalEventHandler<IndexTemplateCreatedEventData>, ITransientDependency
{
    private readonly IIndexNameProvider _indexNameProvider;

    public IndexCreatedEventHandler(IIndexNameProvider indexNameProvider)
    {
        _indexNameProvider = indexNameProvider;
    }

    public async Task HandleEventAsync(IndexCreatedEventData eventData)
    {
        _indexNameProvider.AddIndexName(eventData.IndexName);
    }

    public async Task HandleEventAsync(IndexTemplateCreatedEventData eventData)
    {
        _indexNameProvider.AddIndexTemplate(eventData.TemplateName);
    }
}