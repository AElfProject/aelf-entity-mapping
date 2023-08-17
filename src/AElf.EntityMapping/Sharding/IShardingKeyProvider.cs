using System.Collections.ObjectModel;
using System.Linq.Expressions;

namespace AElf.EntityMapping.Sharding;

public interface IShardingKeyProvider<TEntity> where TEntity : class
{
    public List<ShardingKeyInfo<TEntity>> GetShardingKeyByEntity();
    
    public Task<List<string>> GetCollectionNameAsync(List<CollectionNameCondition> conditions);

    public Task<string> GetCollectionNameAsync(TEntity entity);
    
    public Task<List<string>> GetCollectionNameAsync(List<TEntity> entities);
    
    public bool IsShardingCollection();

}