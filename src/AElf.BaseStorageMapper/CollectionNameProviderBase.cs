using Microsoft.Extensions.Options;
using Volo.Abp.DependencyInjection;

namespace AElf.BaseStorageMapper;

public abstract class CollectionNameProviderBase<TEntity> : ICollectionNameProvider<TEntity>
    where TEntity : class
{
    public IAbpLazyServiceProvider LazyServiceProvider { get; set; }

    protected AElfBaseStorageMapperOptions AElfBaseStorageMapperOptions => LazyServiceProvider
        .LazyGetRequiredService<IOptionsSnapshot<AElfBaseStorageMapperOptions>>().Value;

    public string GetFullCollectionName()
    {
        var collectionName = GetCollectionName();
        var fullCollectionName = string.IsNullOrWhiteSpace(AElfBaseStorageMapperOptions.CollectionPrefix)
            ? collectionName
            : $"{AElfBaseStorageMapperOptions.CollectionPrefix}.{collectionName}";

        return FormatCollectionName(fullCollectionName);
    }

    protected abstract string GetCollectionName();
    
    protected abstract string FormatCollectionName(string name);
}