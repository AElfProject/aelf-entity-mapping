using AElf.BaseStorageMapper.Entities;

namespace AElf.BaseStorageMapper.Tests;

public class BlockIndex:BlockBase,IAElfEntity
{
    public List<string> TransactionIds { get; set; }
    public int LogEventCount { get; set; }
}