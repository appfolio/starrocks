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

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileScanTask;

public final class IcebergRewriteFileSelector {
    // https://iceberg.apache.org/spec/#reserved-field-ids
    private static final int FILE_PATH_FIELD_ID = 2147483546;

    private IcebergRewriteFileSelector() {
    }

    // A file above the threshold is still picked when a partition-level position delete applies to it, so the
    // delete can be eliminated. A file-level one can be skipped. Equality deletes are only eliminated by rewrite-all.
    public static boolean isRewriteCandidate(FileScanTask fileScanTask, long fileSizeThreshold, boolean allFiles) {
        if (allFiles || fileScanTask.file().fileSizeInBytes() <= fileSizeThreshold) {
            return true;
        }
        for (DeleteFile del : fileScanTask.deletes()) {
            if (del.content() == FileContent.POSITION_DELETES && del.referencedDataFile() == null
                    && del.lowerBounds() != null && del.upperBounds() != null
                    && !del.lowerBounds().get(FILE_PATH_FIELD_ID).equals(del.upperBounds().get(FILE_PATH_FIELD_ID))) {
                return true;
            }
        }
        return false;
    }
}
