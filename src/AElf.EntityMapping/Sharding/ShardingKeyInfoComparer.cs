namespace AElf.EntityMapping.Sharding;

public class ShardingKeyInfoComparer<TEntity> : IComparer<ShardingKeyInfo<TEntity>> where TEntity : class
{
    public int Compare(ShardingKeyInfo<TEntity> x, ShardingKeyInfo<TEntity> y)
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