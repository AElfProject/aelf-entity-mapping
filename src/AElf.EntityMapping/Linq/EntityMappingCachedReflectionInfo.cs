using System.Reflection;

namespace System.Linq;

public static class EntityMappingCachedReflectionInfo
{
    private static MethodInfo? _afterMethodInfo;
    
    public static MethodInfo AfterMethodInfo(Type source) =>
        (_afterMethodInfo ??= new Func<IQueryable<object>, object[], IQueryable<object>>(QueryableExtensions.After).GetMethodInfo().GetGenericMethodDefinition())
        .MakeGenericMethod(source);
}