using System.Collections;
using Volo.Abp.Domain.Entities;

namespace AElf.BaseStorageMapper.Elasticsearch.Linq;

public interface IElasticsearchQueryable<T> : IQueryable<T>
{

}