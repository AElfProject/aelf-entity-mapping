namespace AElf.EntityMapping.Sharding;

public class ShardProviderEntityComparer<TEntity> : IComparer<ShardProviderEntity<TEntity>> where TEntity : class
{
    public int Compare(ShardProviderEntity<TEntity> x, ShardProviderEntity<TEntity> y)
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