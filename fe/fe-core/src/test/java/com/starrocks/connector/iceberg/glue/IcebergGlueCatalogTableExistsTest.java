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

package com.starrocks.connector.iceberg.glue;

import com.starrocks.qe.ConnectContext;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class IcebergGlueCatalogTableExistsTest {
    private static final String CATALOG_ID = "123456789012";

    @Mocked
    private GlueCatalog glueCatalog;

    private static class FakeGlueClient implements GlueClient {
        private final Function<GetTableRequest, Table> getTable;
        private final List<GetTableRequest> requests = new ArrayList<>();
        private boolean closed = false;

        FakeGlueClient(Function<GetTableRequest, Table> getTable) {
            this.getTable = getTable;
        }

        @Override
        public GetTableResponse getTable(GetTableRequest request) {
            requests.add(request);
            return GetTableResponse.builder().table(getTable.apply(request)).build();
        }

        @Override
        public String serviceName() {
            return "glue";
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private static Table glueTable(Map<String, String> parameters) {
        return Table.builder().name("t").parameters(parameters).build();
    }

    private static Map<String, String> params(String... keysAndValues) {
        Map<String, String> parameters = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            parameters.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return parameters;
    }

    private static IcebergGlueCatalog catalog(GlueClient client) {
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.region", "us-east-1");
        properties.put(AwsProperties.GLUE_CATALOG_ID, CATALOG_ID);
        return new IcebergGlueCatalog("test_catalog", new Configuration(), properties, () -> client);
    }

    @Test
    public void testIcebergTableExistsWithoutLoadingIt() {
        FakeGlueClient client = new FakeGlueClient(request -> glueTable(params(
                "table_type", "ICEBERG", "metadata_location", "s3://bucket/db/t/metadata/00001-x.metadata.json")));

        Assertions.assertTrue(catalog(client).tableExists(new ConnectContext(), "db", "t"));

        Assertions.assertEquals(1, client.requests.size());
        GetTableRequest request = client.requests.get(0);
        Assertions.assertEquals(CATALOG_ID, request.catalogId());
        Assertions.assertEquals("db", request.databaseName());
        Assertions.assertEquals("t", request.name());
        new Verifications() {
            {
                glueCatalog.loadTable((TableIdentifier) any);
                times = 0;
                glueCatalog.tableExists((TableIdentifier) any);
                times = 0;
            }
        };
    }

    @Test
    public void testMissingTableDoesNotExist() {
        FakeGlueClient client = new FakeGlueClient(request -> {
            throw EntityNotFoundException.builder().message("not found").build();
        });

        Assertions.assertFalse(catalog(client).tableExists(new ConnectContext(), "db", "t"));
    }

    @Test
    public void testNonIcebergTableDoesNotExist() {
        Assertions.assertFalse(catalog(new FakeGlueClient(request -> glueTable(params(
                "table_type", "EXTERNAL_TABLE", "metadata_location", "s3://bucket/x.metadata.json"))))
                .tableExists(new ConnectContext(), "db", "t"));
        Assertions.assertFalse(catalog(new FakeGlueClient(request -> Table.builder().name("t").build()))
                .tableExists(new ConnectContext(), "db", "t"));
    }

    @Test
    public void testIcebergTableWithoutMetadataLocationDoesNotExist() {
        Assertions.assertFalse(catalog(new FakeGlueClient(request -> glueTable(params("table_type", "iceberg"))))
                .tableExists(new ConnectContext(), "db", "t"));
        Assertions.assertFalse(catalog(new FakeGlueClient(request -> glueTable(params(
                "table_type", "iceberg", "metadata_location", "")))).tableExists(new ConnectContext(), "db", "t"));
    }

    @Test
    public void testOtherGlueErrorsPropagate() {
        FakeGlueClient client = new FakeGlueClient(request -> {
            throw GlueException.builder().message("access denied").statusCode(403).build();
        });

        Assertions.assertThrows(GlueException.class,
                () -> catalog(client).tableExists(new ConnectContext(), "db", "t"));
    }

    @Test
    public void testClientIsCreatedOnceAndClosedOnShutdown() {
        FakeGlueClient client = new FakeGlueClient(request -> glueTable(params(
                "table_type", "iceberg", "metadata_location", "s3://bucket/x.metadata.json")));
        int[] created = {0};
        Map<String, String> properties = new HashMap<>();
        properties.put("aws.region", "us-east-1");
        IcebergGlueCatalog catalog = new IcebergGlueCatalog("test_catalog", new Configuration(), properties, () -> {
            created[0]++;
            return client;
        });

        catalog.shutdown();
        Assertions.assertEquals(0, created[0]);
        Assertions.assertFalse(client.closed);

        catalog.tableExists(new ConnectContext(), "db", "t");
        catalog.tableExists(new ConnectContext(), "db", "t");
        Assertions.assertEquals(1, created[0]);
        Assertions.assertNull(client.requests.get(0).catalogId());

        catalog.shutdown();
        Assertions.assertTrue(client.closed);
    }
}
