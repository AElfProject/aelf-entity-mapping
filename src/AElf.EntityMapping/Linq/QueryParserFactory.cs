using Remotion.Linq.Parsing.ExpressionVisitors.Transformation;
using Remotion.Linq.Parsing.Structure;
using Remotion.Linq.Parsing.Structure.NodeTypeProviders;

namespace AElf.EntityMapping.Linq;

public class QueryParserFactory
{
    public static QueryParser Create()
    {
        var transformerRegistry = ExpressionTransformerRegistry.CreateDefault();
        var evaluatableExpressionFilter = new EntityMappingEvaluatableExpressionFilter();
        
        var innerProviders = new INodeTypeProvider[]
        {
            MethodInfoBasedNodeTypeRegistry.CreateFromRelinqAssembly(),
            MethodNameBasedNodeTypeRegistry.CreateFromRelinqAssembly(),
            EntityMappingNodeTypeProvider.Create()
        };
        var nodeType = new CompoundNodeTypeProvider (innerProviders);
        
        var expressionTreeParser = new ExpressionTreeParser (
            nodeType, 
            ExpressionTreeParser.CreateDefaultProcessor (transformerRegistry, evaluatableExpressionFilter));
        return new QueryParser(expressionTreeParser);
    }
}