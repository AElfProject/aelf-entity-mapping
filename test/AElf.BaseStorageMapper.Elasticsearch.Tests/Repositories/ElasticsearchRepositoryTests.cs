using Xunit;

namespace AElf.BaseStorageMapper.Elasticsearch.Repositories;

public class ElasticsearchRepositoryTests : AElfElasticsearchTestBase
{
    private readonly IElasticsearchRepository<BlockIndex, string> _elasticsearchRepository;

    public ElasticsearchRepositoryTests()
    {
        _elasticsearchRepository = GetRequiredService<IElasticsearchRepository<BlockIndex, string>>();
    }

    [Fact]
    public async Task Test()
    {
        var querable = await _elasticsearchRepository.GetQueryableAsync();
        var list = querable.Where(q =>
                q.BlockHash == "BlockHash" && q.BlockHeight >= 1 && q.BlockHeight < 100 && q.BlockTime > DateTime.Now)
            .ToList();

        ;
    }
}