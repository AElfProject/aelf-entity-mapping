using Microsoft.Extensions.Options;
using Volo.Abp.DependencyInjection;

namespace AElf.BaseStorageMapper;

public abstract class CollectionNameProviderBase<TEntity> : ICollectionNameProvider<TEntity>
    where TEntity : class
{
    public IAbpLazyServiceProvider LazyServiceProvider { get; set; }

    protected AElfBaseStorageMapperOptions AElfBaseStorageMapperOptions => LazyServiceProvider
        .LazyGetRequiredService<IOptionsSnapshot<AElfBaseStorageMapperOptions>>().Value;

    public List<string> GetFullCollectionName(List<CollectionNameCondition> conditions)
    {
        var collectionNames = GetCollectionName(conditions);
        var fullCollectionNames = string.IsNullOrWhiteSpace(AElfBaseStorageMapperOptions.CollectionPrefix)
            ? collectionNames
            : collectionNames.Select(o => $"{AElfBaseStorageMapperOptions.CollectionPrefix}.{o}");

        return fullCollectionNames.Select(FormatCollectionName).ToList();
    }

    public string GetFullCollectionNameById<TKey>(TKey id)
    {
        var collectionName = GetCollectionNameById(id);
        return FormatCollectionName(collectionName);
    }

    protected abstract List<string> GetCollectionName(List<CollectionNameCondition> conditions);
    
    protected abstract string GetCollectionNameById<TKey>(TKey id);
    
    protected abstract string FormatCollectionName(string name);
}