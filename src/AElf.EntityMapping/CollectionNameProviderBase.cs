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

    public async Task<List<string>> GetFullCollectionNameAsync(List<CollectionNameCondition> conditions)
    {
        var collectionNames = await GetCollectionName(conditions);
        var fullCollectionNames = AddCollectionPrefix(collectionNames);
        return fullCollectionNames.Select(FormatCollectionName).ToList();
    }
    
    public async Task<List<string>> GetFullCollectionNameByEntityAsync(TEntity entity)
    {
        var collectionNames = await GetCollectionNameByEntity(entity);
        var fullCollectionNames = AddCollectionPrefix(collectionNames);
        return fullCollectionNames.Select(FormatCollectionName).ToList();
    }
    
    public async Task<List<string>> GetFullCollectionNameByEntityAsync(List<TEntity> entitys)
    {
        var collectionNames = await GetCollectionNameByEntity(entitys);
        var fullCollectionNames = AddCollectionPrefix(collectionNames);
        return fullCollectionNames.Select(FormatCollectionName).ToList();
    }

    public async Task<string> GetFullCollectionNameByIdAsync<TKey>(TKey id)
    {
        var collectionName = await GetCollectionNameById(id);
        return FormatCollectionName(collectionName);
    }
    
    protected abstract Task<List<string>> GetCollectionName(List<CollectionNameCondition> conditions);

    protected abstract Task<List<string>> GetCollectionNameByEntity(TEntity entity);

    protected abstract Task<List<string>> GetCollectionNameByEntity(List<TEntity> entity);

    protected abstract Task<string> GetCollectionNameById<TKey>(TKey id);
    
    protected abstract string FormatCollectionName(string name);
    
    private List<string> AddCollectionPrefix(List<string> collectionNames)
    {
        return string.IsNullOrWhiteSpace(AElfEntityMappingOptions.CollectionPrefix)
            ? collectionNames
            : collectionNames.Select(o => $"{AElfEntityMappingOptions.CollectionPrefix}.{o}").ToList();
    }
}