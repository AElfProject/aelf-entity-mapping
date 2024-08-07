using System.Reflection;
using Remotion.Linq.Parsing.Structure;

namespace AElf.EntityMapping.Linq;

public class EntityMappingNodeTypeProvider : INodeTypeProvider
{
    public static EntityMappingNodeTypeProvider Create()
    {
        var typeProvider = new EntityMappingNodeTypeProvider();
        typeProvider.Register(AfterExpressionNode.GetSupportedMethods(), typeof(AfterExpressionNode));
        return typeProvider;
    }

    private static readonly Dictionary<MethodInfo, Lazy<MethodInfo[]>> GenericMethodDefinitionCandidates = new();

    public void Register(IEnumerable<MethodInfo> methods, Type nodeType)
    {
        foreach (var method in methods)
        {
            if (method.IsGenericMethod && !method.IsGenericMethodDefinition)
                throw new InvalidOperationException(string.Format(
                    "Cannot register closed generic method '{0}', try to register its generic method definition instead.",
                    new object[1]
                    {
                        (object)method.Name
                    }));
            if (method.DeclaringType.GetTypeInfo().IsGenericType &&
                !method.DeclaringType.GetTypeInfo().IsGenericTypeDefinition)
                throw new InvalidOperationException(string.Format(
                    "Cannot register method '{0}' in closed generic type '{1}', try to register its equivalent in the generic type definition instead.",
                    new object[2]
                    {
                        (object)method.Name,
                        (object)method.DeclaringType
                    }));
            _registeredMethodInfoTypes[method] = nodeType;
        }
    }

    public bool IsRegistered(MethodInfo method)
    {
        return GetNodeType(method) != null;
    }

    public Type GetNodeType(MethodInfo method)
    {
        var methodDefinition = GetRegisterableMethodDefinition(method, throwOnAmbiguousMatch: false);
        if (methodDefinition == null)
            return null;

        Type result;
        _registeredMethodInfoTypes.TryGetValue(methodDefinition, out result);
        return result;
    }

    public static MethodInfo GetRegisterableMethodDefinition(MethodInfo method, bool throwOnAmbiguousMatch)
    {
        var genericMethodDefinition = method.IsGenericMethod ? method.GetGenericMethodDefinition() : method;
        if (!genericMethodDefinition.DeclaringType.GetTypeInfo().IsGenericType)
            return genericMethodDefinition;
        
        Lazy<MethodInfo[]> candidates;
        lock (GenericMethodDefinitionCandidates)
        {
            if (!GenericMethodDefinitionCandidates.TryGetValue(method, out candidates))
            {
                candidates =
                    new Lazy<MethodInfo[]>(() => GetGenericMethodDefinitionCandidates(genericMethodDefinition));
                GenericMethodDefinitionCandidates.Add(method, candidates);
            }
        }

        if (candidates.Value.Length == 1)
            return candidates.Value.Single();

        if (!throwOnAmbiguousMatch)
            return null;

        throw new NotSupportedException(
            string.Format(
                "A generic method definition cannot be resolved for method '{0}' on type '{1}' because a distinct match is not possible. "
                + @"The method can still be registered using the following syntax:

public static readonly NameBasedRegistrationInfo[] SupportedMethodNames = 
    new[] {{
        new NameBasedRegistrationInfo (
            ""{2}"", 
            mi => /* match rule based on MethodInfo */
        )
    }};",
                method,
                genericMethodDefinition.DeclaringType.GetGenericTypeDefinition(),
                method.Name));
    }

    private readonly Dictionary<MethodInfo, Type> _registeredMethodInfoTypes = new Dictionary<MethodInfo, Type>();
    
    public int RegisteredMethodInfoCount => _registeredMethodInfoTypes.Count;

    private static MethodInfo[] GetGenericMethodDefinitionCandidates(MethodInfo referenceMethodDefinition)
    {
        var declaringTypeDefinition = referenceMethodDefinition.DeclaringType.GetGenericTypeDefinition();

        var referenceMethodSignature =
            new[] { new { Name = "returnValue", Type = referenceMethodDefinition.ReturnType } }
                .Concat(referenceMethodDefinition.GetParameters()
                    .Select(p => new { Name = p.Name, Type = p.ParameterType }))
                .ToArray();

        var candidates = declaringTypeDefinition.GetRuntimeMethods()
            .Select(
                m => new
                {
                    Method = m,
                    SignatureNames = new[] { "returnValue" }.Concat(m.GetParameters().Select(p => p.Name)).ToArray(),
                    SignatureTypes = new[] { m.ReturnType }.Concat(m.GetParameters().Select(p => p.ParameterType))
                        .ToArray()
                })
            .Where(c => c.Method.Name == referenceMethodDefinition.Name &&
                        c.SignatureTypes.Length == referenceMethodSignature.Length)
            .ToArray();

        for (var i = 0; i < referenceMethodSignature.Length; i++)
        {
            candidates = candidates
                .Where(c => c.SignatureNames[i] == referenceMethodSignature[i].Name)
                .Where(c => c.SignatureTypes[i] == referenceMethodSignature[i].Type ||
                            c.SignatureTypes[i].GetTypeInfo().ContainsGenericParameters)
                .ToArray();
        }

        return candidates.Select(c => c.Method).ToArray();
    }
}