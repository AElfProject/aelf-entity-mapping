using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace AElf.EntityMapping.Sharding;

public interface IShardingCollectionTailProvider<TEntity> where TEntity : class
{
    public Task AddShardingCollectionTailAsync(string entityName, string keys, long tail);

    public Task<Tuple<long, List<ShardingCollectionTail>>> GetShardingCollectionTailAsync(ShardingCollectionTail shardingCollectionTail);
}