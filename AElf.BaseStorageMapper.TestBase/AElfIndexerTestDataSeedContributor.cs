using Volo.Abp.Data;
using Volo.Abp.DependencyInjection;

namespace AElf.BaseStorageMapper.TestBase;

public class AElfIndexerTestDataSeedContributor : IDataSeedContributor, ITransientDependency
{
    public Task SeedAsync(DataSeedContext context)
    {
        /* Seed additional test data... */

        return Task.CompletedTask;
    }
}
