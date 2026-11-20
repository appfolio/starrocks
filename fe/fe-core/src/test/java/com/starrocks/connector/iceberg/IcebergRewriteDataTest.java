// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

public class IcebergRewriteDataTest extends TableTestBase {

    private static final DataFile LARGE_FILE_A =
            DataFiles.builder(SPEC_A)
                    .withPath("/path/to/data-a-large.parquet")
                    .withFileSizeInBytes(1000)
                    .withPartitionPath("data_bucket=0")
                    .withRecordCount(2)
                    .build();

    private void append(DataFile... files) {
        AppendFiles append = mockedNativeTableA.newFastAppend();
        for (DataFile file : files) {
            append.appendFile(file);
        }
        append.commit();
    }

    private IcebergRewriteData build(boolean skipSingleFilePartitions, long threshold) {
        List<FileScanTask> tasks = Lists.newArrayList(mockedNativeTableA.newScan().planFiles());
        IcebergConnectorScanRangeSource source = Mockito.mock(IcebergConnectorScanRangeSource.class);
        Mockito.when(source.getSourceFileScanOutputs(anyInt(), anyLong(), eq(true)))
                .thenReturn(tasks)
                .thenReturn(Collections.emptyList());
        IcebergRewriteData rewriteData = new IcebergRewriteData();
        rewriteData.addSource(source);
        rewriteData.setSkipSingleFilePartitions(skipSingleFilePartitions, mockedNativeTableA.spec().specId());
        rewriteData.buildNewScanNodeRange(threshold, false);
        return rewriteData;
    }

    private static Set<String> locations(Set<DataFile> files) {
        Set<String> result = Sets.newHashSet();
        files.forEach(f -> result.add(f.location()));
        return result;
    }

    private static int groups(IcebergRewriteData rewriteData) {
        int n = 0;
        while (rewriteData.hasMoreTaskGroup()) {
            rewriteData.nextTaskGroup();
            n++;
        }
        return n;
    }

    @Test
    public void testSkipsPartitionWithOneFileAndNoDeletes() {
        append(FILE_A, FILE_A_1, FILE_A_2);

        IcebergRewriteData rewriteData = build(true, 256);

        Assertions.assertEquals(Sets.newHashSet(FILE_A.location(), FILE_A_1.location()),
                locations(rewriteData.getPickedDataFiles()));
        Assertions.assertEquals(1, groups(rewriteData));
    }

    @Test
    public void testRewritesEveryCandidateWhenSkipIsOff() {
        append(FILE_A, FILE_A_1, FILE_A_2);

        IcebergRewriteData rewriteData = build(false, 256);

        Assertions.assertEquals(Sets.newHashSet(FILE_A.location(), FILE_A_1.location(), FILE_A_2.location()),
                locations(rewriteData.getPickedDataFiles()));
        Assertions.assertEquals(2, groups(rewriteData));
    }

    @Test
    public void testKeepsSingleFileWithDeletesAndDropsItsDeletes() {
        append(FILE_A);
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_DELETES).commit();

        IcebergRewriteData rewriteData = build(true, 256);

        Assertions.assertEquals(Sets.newHashSet(FILE_A.location()), locations(rewriteData.getPickedDataFiles()));
        Assertions.assertEquals(1, rewriteData.getDanglingPosDeleteFiles().size());
        Assertions.assertEquals(FILE_A_DELETES.location(),
                rewriteData.getDanglingPosDeleteFiles().iterator().next().location());
    }

    @Test
    public void testKeepsDeleteThatStillAppliesToAFileLeftAlone() {
        append(FILE_A, LARGE_FILE_A);
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_DELETES).commit();

        IcebergRewriteData rewriteData = build(true, 100);

        Assertions.assertEquals(Sets.newHashSet(FILE_A.location()), locations(rewriteData.getPickedDataFiles()));
        Assertions.assertTrue(rewriteData.getDanglingPosDeleteFiles().isEmpty());
    }
}
