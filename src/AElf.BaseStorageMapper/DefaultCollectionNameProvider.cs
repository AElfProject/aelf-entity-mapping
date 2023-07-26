namespace AElf.BaseStorageMapper;

public class DefaultCollectionNameProvider<TEntity>:CollectionNameProviderBase<TEntity>
    where TEntity : class
{
    protected override string GetCollectionName()
    {
        return typeof(TEntity).Name;
    }

    protected override string FormatCollectionName(string name)
    {
        return name;
    }
}