namespace AElf.BaseStorageMapper.Tests;

public class BlockIndex:BlockBase,IIndexBuild
{
    public List<string> TransactionIds { get; set; }
    public int LogEventCount { get; set; }
}