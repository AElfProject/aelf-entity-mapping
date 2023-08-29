using Nest;

namespace AElf.EntityMapping.Elasticsearch.Linq
{
    public class AndNode : Node
    {
        public Node Left { get; set; }
        public Node Right { get; set; }

        public override QueryContainer Accept(INodeVisitor visitor)
        {
            return visitor.Visit(this);
        }
    }
}