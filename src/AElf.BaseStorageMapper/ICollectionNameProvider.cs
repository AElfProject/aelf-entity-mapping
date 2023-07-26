using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper;

public interface ICollectionNameProvider<TEntity>
    where TEntity : class
{
    string GetFullCollectionName();
}