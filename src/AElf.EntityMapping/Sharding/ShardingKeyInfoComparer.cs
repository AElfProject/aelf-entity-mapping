namespace AElf.EntityMapping.Sharding;

public class ShardingKeyInfoComparer<TEntity> : IComparer<ShardingKey<TEntity>> where TEntity : class
{
    public int Compare(ShardingKey<TEntity> x, ShardingKey<TEntity> y)
    {
        if (x == null && y == null)
        {
            return 0;
        }
        else if (x == null)
        {
            return -1;
        }
        else if (y == null)
        {
            return 1;
        }
        else
        {
            return x.Order.CompareTo(y.Order);
        }
    }
}