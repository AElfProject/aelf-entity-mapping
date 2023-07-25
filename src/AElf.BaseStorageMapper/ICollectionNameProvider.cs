namespace AElf.BaseStorageMapper;

public interface ICollectionNameProvider<TEntity>
{
    Task<string> GetCollectionNameAsync();
}