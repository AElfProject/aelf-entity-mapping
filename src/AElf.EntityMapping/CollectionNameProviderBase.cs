using AElf.EntityMapping.Options;
using Microsoft.Extensions.Options;
using Volo.Abp.DependencyInjection;

namespace AElf.EntityMapping;

public abstract class CollectionNameProviderBase<TEntity> : ICollectionNameProvider<TEntity>
    where TEntity : class
{
    public IAbpLazyServiceProvider LazyServiceProvider { get; set; }

    protected AElfEntityMappingOptions AElfEntityMappingOptions => LazyServiceProvider
        .LazyGetRequiredService<IOptionsSnapshot<AElfEntityMappingOptions>>().Value;

    public List<string> GetFullCollectionName(List<CollectionNameCondition> conditions)
    {
        var collectionNames = GetCollectionName(conditions);
        /*var fullCollectionNames = string.IsNullOrWhiteSpace(AElfEntityMappingOptions.CollectionPrefix)
            ? collectionNames
            : collectionNames.Select(o => $"{AElfEntityMappingOptions.CollectionPrefix}.{o}");

        return fullCollectionNames.Select(FormatCollectionName).ToList();*/
        return collectionNames;
    }
    
    public List<string> GetFullCollectionNameByEntity(TEntity entity)
    {
        var collectionNames = GetCollectionNameByEntity(entity);
        /*var fullCollectionNames = string.IsNullOrWhiteSpace(AElfEntityMappingOptions.CollectionPrefix)
            ? collectionNames
            : collectionNames.Select(o => $"{AElfEntityMappingOptions.CollectionPrefix}.{o}");*/
        /*var fullCollectionNames = string.IsNullOrWhiteSpace(AElfEntityMappingOptions.CollectionPrefix)
            ? collectionNames
            : collectionNames.Select(o => $"{AElfEntityMappingOptions.CollectionPrefix}.{o}");

        return fullCollectionNames.Select(FormatCollectionName).ToList();*/
        return collectionNames;
    }

    public string GetFullCollectionNameById<TKey>(TKey id)
    {
        var collectionName = GetCollectionNameById(id);
        return FormatCollectionName(collectionName);
    }
    
    protected abstract List<string> GetCollectionName(List<CollectionNameCondition> conditions);

    protected abstract List<string> GetCollectionNameByEntity(TEntity entity);

    protected abstract string GetCollectionNameById<TKey>(TKey id);
    
    protected abstract string FormatCollectionName(string name);
}