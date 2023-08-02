using Volo.Abp.Data;
using Volo.Abp.DependencyInjection;

namespace AElf.EntityMapping.TestBase;

public class AElfEntityMappingTestDataSeedContributor : IDataSeedContributor, ITransientDependency
{
    public Task SeedAsync(DataSeedContext context)
    {
        /* Seed additional test data... */

        return Task.CompletedTask;
    }
}
