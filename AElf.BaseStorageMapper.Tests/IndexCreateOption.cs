namespace AElf.LinqToElasticSearch;

public class IndexCreateOption
{
    public List<Type> Modules { get; } = new List<Type>();

    public void AddModule(Type module)
    {
        if (this.Modules.Contains(module))
            return;
        this.Modules.Add(module);
    }

    public void AddModules(List<Type> modules) => modules.ForEach(new Action<Type>(this.AddModule));
}
