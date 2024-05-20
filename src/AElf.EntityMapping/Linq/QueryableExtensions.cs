using System.Linq.Expressions;

namespace System.Linq;

public static class QueryableExtensions
{
    public static IQueryable<TSource> After<TSource>(this IQueryable<TSource> source, object[] position)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(position);
        
        return source.Provider.CreateQuery<TSource>(
            Expression.Call(
                null,
                EntityMappingCachedReflectionInfo.AfterMethodInfo(typeof(TSource)),
                source.Expression, Expression.Constant(position)
            ));
    }
}