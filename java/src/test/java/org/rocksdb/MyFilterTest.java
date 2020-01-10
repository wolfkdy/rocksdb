package org.rocksdb;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyFilterTest
{
    @Rule
    public TemporaryFolder dbFolder = new TemporaryFolder();

    @Test
    public void TestManualFilter()
        throws RocksDBException
    {
        CassandraPartitionMetaMergeOperator partitionMetaMergeOperator = new CassandraPartitionMetaMergeOperator(864000);
        CassandraValueMergeOperator mergeOperator = new CassandraValueMergeOperator(864000, 0);
        Cache cache = new LRUCache(1024 * 10* 1024 * 1024L);
        Cache metaCache = new LRUCache(1024 * 5 * 1024 * 1024L);

        BlockBasedTableConfig tableOptions = new BlockBasedTableConfig();
        tableOptions.setFilter(new BloomFilter(10, false));
        tableOptions.setBlockCache(cache);
        tableOptions.setCacheIndexAndFilterBlocks(false);
        tableOptions.setPinL0FilterAndIndexBlocksInCache(false);
        tableOptions.setIndexType(IndexType.kBinarySearch);


        BlockBasedTableConfig metaTableOption = new BlockBasedTableConfig();
        metaTableOption.setFilter(new BloomFilter(10, false));
        metaTableOption.setBlockCache(metaCache);
        metaTableOption.setIndexType(IndexType.kBinarySearch);

        DBOptions dbOptions = new DBOptions();
        SstFileManager sstFileManager = new SstFileManager(Env.getDefault());

        // sstFilemanager options
        sstFileManager.setDeleteRateBytesPerSecond(100 * 1024 * 1024);

        Statistics  stats = new Statistics();
        stats.setStatsLevel(StatsLevel.EXCEPT_DETAILED_TIMERS);
        // db options
        dbOptions.setCreateIfMissing(true);
        dbOptions.setCreateMissingColumnFamilies(true);
        dbOptions.setAllowConcurrentMemtableWrite(true);
        dbOptions.setEnableWriteThreadAdaptiveYield(true);
        dbOptions.setBytesPerSync(1024 * 1024);
        dbOptions.setWalBytesPerSync(1024 * 1024);
        dbOptions.setBaseBackgroundCompactions(4);
        dbOptions.setMaxSubcompactions(8);
        dbOptions.setMaxBackgroundJobs(32);
        dbOptions.setStatistics(stats);
        RateLimiter rateLimiter = new RateLimiter(1024L * 1024L * 16);
        dbOptions.setRateLimiter(rateLimiter);
        dbOptions.setSstFileManager(sstFileManager);
        dbOptions.setMaxTotalWalSize((64 + 64 /2) * 1024 * 1024L);

        List<ColumnFamilyDescriptor> cfDescs = new ArrayList<>(2);
        ArrayList<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(2);

        // config meta column family
        ColumnFamilyOptions metaCfOptions = new ColumnFamilyOptions();
        metaCfOptions.setNumLevels(5);
        metaCfOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        metaCfOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
        metaCfOptions.setMergeOperator(partitionMetaMergeOperator);
        metaCfOptions.setMaxWriteBufferNumber(2);
        metaCfOptions.setWriteBufferSize(16 * 1024 * 1024L);
        metaCfOptions.setTableFormatConfig(metaTableOption);
        ColumnFamilyDescriptor metaCfDescriptor = new ColumnFamilyDescriptor("meta".getBytes(), metaCfOptions);


        // config default column family for holding data
        ColumnFamilyOptions dataCfOptions = new ColumnFamilyOptions();
        dataCfOptions.setNumLevels(5);
        dataCfOptions.setCompressionType(CompressionType.LZ4_COMPRESSION);
        dataCfOptions.setBottommostCompressionType(CompressionType.LZ4_COMPRESSION);
        dataCfOptions.setWriteBufferSize(64 * 1024 * 1024L);
        dataCfOptions.setMaxWriteBufferNumber(32);
        dataCfOptions.setMaxBytesForLevelBase(1024 * 1024 * 1024L);
        dataCfOptions.setSoftPendingCompactionBytesLimit(64 * 1073741824L);
        dataCfOptions.setHardPendingCompactionBytesLimit(8 * 64 * 1073741824L);
        dataCfOptions.setCompactionPriority(CompactionPriority.MinOverlappingRatio);
        dataCfOptions.setLevel0SlowdownWritesTrigger(512);
        dataCfOptions.setLevel0StopWritesTrigger(1024);
        dataCfOptions.setLevelCompactionDynamicLevelBytes(true);
        dataCfOptions.setMergeOperator(mergeOperator);
//        dataCfOptions.setCompactionFilter(this.compactionFilter);
        dataCfOptions.setTableFormatConfig(tableOptions);
        ColumnFamilyDescriptor dataCfDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, dataCfOptions);

        cfDescs.add(dataCfDescriptor);
        cfDescs.add(metaCfDescriptor);

        RocksDB rocksDB = RocksDB.open(dbOptions, dbFolder.getRoot().getAbsolutePath(), cfDescs, columnFamilyHandles);
        ReadOptions readOptions = new ReadOptions().setIgnoreRangeDeletions(false);
        RocksIterator iterator = rocksDB.newIterator(columnFamilyHandles.get(0), readOptions);
        iterator.seekToFirst();
    }

}
