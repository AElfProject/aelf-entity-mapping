﻿using System.Collections.ObjectModel;
using System.Linq.Expressions;
using AElf.EntityMapping.Elasticsearch.Options;
using AElf.EntityMapping.Linq;
using AElf.EntityMapping.Options;
using Microsoft.Extensions.Options;
using Nest;
using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ResultOperators;

namespace AElf.EntityMapping.Elasticsearch.Linq
{
    public class ElasticsearchGeneratorQueryModelVisitor<TU> : QueryModelVisitorBase
    {
        private readonly PropertyNameInferrerParser _propertyNameInferrerParser;
        private readonly INodeVisitor _nodeVisitor;
        private readonly ElasticsearchOptions _elasticsearchOptions;
        private QueryAggregator QueryAggregator { get; set; } = new QueryAggregator();

        public ElasticsearchGeneratorQueryModelVisitor(PropertyNameInferrerParser propertyNameInferrerParser,
            ElasticsearchOptions elasticsearchOptions)
        {
            _propertyNameInferrerParser = propertyNameInferrerParser;
            _nodeVisitor = new NodeVisitor();
            _elasticsearchOptions = elasticsearchOptions;
        }

        public QueryAggregator GenerateElasticQuery<T>(QueryModel queryModel)
        {
            QueryAggregator = new QueryAggregator();
            VisitQueryModel(queryModel);
            return QueryAggregator;
        }

        public override void VisitQueryModel(QueryModel queryModel)
        {
            queryModel.SelectClause.Accept(this, queryModel);
            queryModel.MainFromClause.Accept(this, queryModel);
            VisitBodyClauses(queryModel.BodyClauses, queryModel);
            VisitResultOperators(queryModel.ResultOperators, queryModel);
        }

        public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel)
        {
            if (fromClause.FromExpression is SubQueryExpression subQueryExpression)
            {
                VisitQueryModel(subQueryExpression.QueryModel);
            }

            base.VisitMainFromClause(fromClause, queryModel);
        }

        public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index)
        {
            var tree = new GeneratorExpressionTreeVisitor<TU>(_propertyNameInferrerParser, _elasticsearchOptions);
            tree.Visit(whereClause.Predicate);
            if (QueryAggregator.Query == null)
            {
                var node = tree.QueryMap[whereClause.Predicate];
                QueryAggregator.Query = node.Accept(_nodeVisitor);
            }
            else
            {
                var left = QueryAggregator.Query;
                var right = tree.QueryMap[whereClause.Predicate].Accept(_nodeVisitor);

                var query = new BoolQuery
                {
                    Must = new[] { left, right }
                };

                QueryAggregator.Query = query;
            }

            base.VisitWhereClause(whereClause, queryModel, index);
        }

        protected override void VisitResultOperators(ObservableCollection<ResultOperatorBase> resultOperators,
            QueryModel queryModel)
        {
            foreach (var resultOperator in resultOperators)
            {
                switch (resultOperator)
                {
                    case SkipResultOperator skipResultOperator:
                        QueryAggregator.Skip = skipResultOperator.GetConstantCount();
                        break;
                    case TakeResultOperator takeResultOperator:
                        QueryAggregator.Take = takeResultOperator.GetConstantCount();
                        break;
                    case GroupResultOperator groupResultOperator:
                    {
                        var members = new List<Tuple<string, Type>>();

                        switch (groupResultOperator.KeySelector)
                        {
                            case MemberExpression memberExpression:
                                members.Add(new Tuple<string, Type>(GetFullNameKey(memberExpression), memberExpression.Type));
                                break;
                            case NewExpression newExpression:
                                members.AddRange(newExpression.Arguments
                                    .Cast<MemberExpression>()
                                    .Select(memberExpression => new Tuple<string, Type>(GetFullNameKey(memberExpression), memberExpression.Type)));
                                break;
                        }

                        members.ForEach(property => { QueryAggregator.GroupByExpressions.Add(new GroupByProperties(property.Item1, property.Item2)); });
                        break;
                    }
                    case AfterResultOperator afterResultOperator:
                        QueryAggregator.After = afterResultOperator.GetConstantPosition();
                        break;
                }
            }

            base.VisitResultOperators(resultOperators, queryModel);
        }

        private string GetFullNameKey(MemberExpression memberExpression)
        {
            var key = _propertyNameInferrerParser.Parser(memberExpression.Member.Name);
            while (memberExpression.Expression != null)
            {
                memberExpression = memberExpression.Expression as MemberExpression;
                if (memberExpression == null)
                {
                    break;
                }

                key = _propertyNameInferrerParser.Parser(memberExpression.Member.Name) + "." + key;
                return key;
            }

            return key;
        }


        public override void VisitOrderByClause(OrderByClause orderByClause, QueryModel queryModel, int index)
        {
            foreach (var ordering in orderByClause.Orderings)
            {
                var memberExpression = (MemberExpression)ordering.Expression;
                var direction = orderByClause.Orderings[0].OrderingDirection;
                //get full property path if there is sub object
                string propertyName = GetFullPropertyPath(memberExpression);

                if (!string.IsNullOrEmpty(propertyName))
                {
                    var type = memberExpression.Type; 
                    QueryAggregator.OrderByExpressions.Add(new OrderProperties(propertyName, type, direction));
                }
            }

            base.VisitOrderByClause(orderByClause, queryModel, index);
        }
        
        private string GetFullPropertyPath(Expression expression)
        {
            switch (expression)
            {
                case MemberExpression memberExpression:
                    var parentPath = GetFullPropertyPath(memberExpression.Expression);
                    var currentMemberName = _propertyNameInferrerParser.Parser(memberExpression.Member.Name);
                    return string.IsNullOrEmpty(parentPath) ? currentMemberName : $"{parentPath}.{currentMemberName}";

                case MethodCallExpression methodCallExpression:
                    // Handles method calls like 'get_Item', which are usually associated with indexed access to collections
                    if (methodCallExpression.Method.Name.Equals("get_Item") && methodCallExpression.Object != null)
                    {
                        // Assuming this is an indexed access to an array or list, we will ignore the index and use only the name of the collection
                        var collectionPath = GetFullPropertyPath(methodCallExpression.Object);
                        return collectionPath; // Returns the path of the collection directly, without adding an index
                    }
                    break;
            }

            return null;
        }
    }
}