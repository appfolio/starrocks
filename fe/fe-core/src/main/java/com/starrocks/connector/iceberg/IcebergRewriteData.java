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

import com.starrocks.connector.RemoteFileInfo;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.util.StructLikeWrapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;


public class IcebergRewriteData {

    // One source per data scan node. A merge-on-read plan has two mutually exclusive data branches
    // (with / without equality deletes), so candidates must be collected from all of them.
    private final List<IcebergConnectorScanRangeSource> sources = new ArrayList<>();
    private Map<StructLikeWrapper, List<TaskGroup>> partitionedTasks;
    private int maxScanRangeLength = 100;
    private long batchSize = 10L * 1024 * 1024 * 1024;
    private boolean skipSingleFilePartitions = false;
    private int currentSpecId = -1;
    private Iterator<List<TaskGroup>> outer;
    private Iterator<TaskGroup> inner = Collections.emptyIterator();

    private final Set<DataFile> pickedDataFiles = new HashSet<>();
    // Position deletes that apply only to picked data files. Rewriting those files leaves the deletes
    // applying to nothing, so the commit can drop them, including for picked files that hold no live rows.
    private final Set<DeleteFile> danglingPosDeleteFiles = new HashSet<>();
    private final Set<DeleteFile> pickedEqDeleteFiles = new HashSet<>();

    public void addSource(IcebergConnectorScanRangeSource source) {
        if (source != null) {
            this.sources.add(source);
        }
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public void setMaxScanRangeLength(int maxScanRangeLength) {
        this.maxScanRangeLength = maxScanRangeLength;
    }

    // Leave a partition alone when its only candidate is one file with no deletes, as Trino's OPTIMIZE does.
    // Files written under an older spec are never skipped.
    public void setSkipSingleFilePartitions(boolean skipSingleFilePartitions, int currentSpecId) {
        this.skipSingleFilePartitions = skipSingleFilePartitions;
        this.currentSpecId = currentSpecId;
    }

    public void buildNewScanNodeRange(long fileSizeThreshold, boolean allFiles) {
        List<FileScanTask> candidates = new ArrayList<>();
        Set<DeleteFile> posDeletesOnUnpickedFiles = new HashSet<>();
        for (IcebergConnectorScanRangeSource source : sources) {
            List<FileScanTask> tasks;
            do {
                tasks = source.getSourceFileScanOutputs(maxScanRangeLength, fileSizeThreshold, true);
                for (FileScanTask task : tasks) {
                    if (IcebergRewriteFileSelector.isRewriteCandidate(task, fileSizeThreshold, allFiles)) {
                        candidates.add(task);
                    } else {
                        posDeletesOnUnpickedFiles.addAll(posDeletes(task));
                    }
                }
            } while (!tasks.isEmpty());
        }

        Map<StructLikeWrapper, List<FileScanTask>> byPartition = new HashMap<>();
        for (FileScanTask task : candidates) {
            byPartition.computeIfAbsent(partitionOf(task), k -> new ArrayList<>()).add(task);
        }

        partitionedTasks = new HashMap<>();
        for (Map.Entry<StructLikeWrapper, List<FileScanTask>> entry : byPartition.entrySet()) {
            List<FileScanTask> tasks = entry.getValue();
            if (!allFiles && skipSingleFilePartitions && isSingleFileWithoutDeletes(tasks)) {
                continue;
            }
            List<TaskGroup> taskGroups = new ArrayList<>();
            for (FileScanTask task : tasks) {
                pickedDataFiles.add(task.file());
                for (DeleteFile del : task.deletes()) {
                    if (del.content() == FileContent.POSITION_DELETES) {
                        danglingPosDeleteFiles.add(del);
                    } else if (del.content() == FileContent.EQUALITY_DELETES) {
                        pickedEqDeleteFiles.add(del);
                    }
                }
                addToGroup(taskGroups, task);
            }
            partitionedTasks.put(entry.getKey(), taskGroups);
        }
        danglingPosDeleteFiles.removeAll(posDeletesOnUnpickedFiles);
        this.outer = partitionedTasks.values().iterator();
    }

    // A large file may be split into several tasks, so count files, not tasks.
    private boolean isSingleFileWithoutDeletes(List<FileScanTask> tasks) {
        Set<String> files = tasks.stream().map(t -> t.file().location()).collect(Collectors.toSet());
        FileScanTask only = tasks.get(0);
        return files.size() == 1 && only.deletes().isEmpty() && only.spec().specId() == currentSpecId;
    }

    private void addToGroup(List<TaskGroup> taskGroups, FileScanTask task) {
        for (TaskGroup tg : taskGroups) {
            if (tg.getTotalSize() + task.file().fileSizeInBytes() <= batchSize) {
                tg.addTask(task);
                return;
            }
        }
        TaskGroup tg = new TaskGroup();
        tg.addTask(task);
        taskGroups.add(tg);
    }

    private static StructLikeWrapper partitionOf(FileScanTask task) {
        PartitionSpec spec = task.spec();
        return StructLikeWrapper.forType(spec.partitionType()).copyFor(task.file().partition());
    }

    private static List<DeleteFile> posDeletes(FileScanTask task) {
        return task.deletes().stream()
                .filter(del -> del.content() == FileContent.POSITION_DELETES)
                .collect(Collectors.toList());
    }

    public Set<DataFile> getPickedDataFiles() {
        return pickedDataFiles;
    }

    public Set<DeleteFile> getDanglingPosDeleteFiles() {
        return danglingPosDeleteFiles;
    }

    public Set<DeleteFile> getPickedEqDeleteFiles() {
        return pickedEqDeleteFiles;
    }

    public boolean hasMoreTaskGroup() {
        while (!inner.hasNext() && outer.hasNext()) {
            inner = outer.next().iterator();
        }
        return inner.hasNext();
    }

    public List<RemoteFileInfo> nextTaskGroup() {
        if (!hasMoreTaskGroup()) {
            throw new NoSuchElementException();
        }
        TaskGroup tg = inner.next();
        return tg.getTasks().stream().map(IcebergRemoteFileInfo::new).collect(Collectors.toList());
    }

}
