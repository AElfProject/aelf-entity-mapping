using Nest;

namespace AElf.EntityMapping.Elasticsearch.Linq
{
    public class NotNode : Node
    {
        public Node Child { get; set; }

        public NotNode(Node child)
        {
            Child = child;
        }

        public override QueryContainer Accept(INodeVisitor visitor)
        {
            return visitor.Visit(this);
        }
    }
}