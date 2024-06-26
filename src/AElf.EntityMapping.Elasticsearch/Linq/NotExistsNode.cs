using Nest;

namespace AElf.EntityMapping.Elasticsearch.Linq
{
    public class NotExistsNode : Node
    {
        public string Field { get; set; }

        public NotExistsNode(string field)
        {
            Field = field;
        }

        public override QueryContainer Accept(INodeVisitor visitor)
        {
            return visitor.Visit(this);
        }
    }
}