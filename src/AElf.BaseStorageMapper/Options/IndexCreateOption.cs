namespace AElf.BaseStorageMapper.Options;

public class IndexCreateOption
{
    public List<Type> Modules { get; } = new List<Type>();

    public void AddModule(Type module)
    {
        if (Modules.Contains(module))
        {
            return;
        }

        Modules.Add(module);
    }
    public void AddModules(List<Type> modules)
    {
        modules.ForEach(AddModule);
    }
}