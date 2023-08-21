using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace AElf.EntityMapping.Sharding;

public interface IShardingCollectionTailProvider<TEntity> where TEntity : class
{
    public Task AddShardingCollectionTailAsync(string tailPrefix, long tail);
    
    public Task<long> GetShardingCollectionTailAsync(string tailPrefix);
}