/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.connect.jdbc.sink.dialect;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import org.junit.rules.ExpectedException;


import static org.junit.Assert.assertEquals;

public class SnowflakeDialectTest extends BaseDialectTest {

  @Rule
  public ExpectedException exception = ExpectedException.none();


  public SnowflakeDialectTest() {
    super(new SnowflakeDialect());
  }

  @Test
  public void dataTypeMappings() {
    verifyDataTypeMapping("NUMBER", Schema.INT8_SCHEMA);
    verifyDataTypeMapping("NUMBER", Schema.INT16_SCHEMA);
    verifyDataTypeMapping("NUMBER", Schema.INT32_SCHEMA);
    verifyDataTypeMapping("NUMBER", Schema.INT64_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT32_SCHEMA);
    verifyDataTypeMapping("FLOAT", Schema.FLOAT64_SCHEMA);
    verifyDataTypeMapping("BOOLEAN", Schema.BOOLEAN_SCHEMA);
    verifyDataTypeMapping("VARCHAR", Schema.STRING_SCHEMA);
    verifyDataTypeMapping("NUMBER(38,0)", Decimal.schema(0));
    verifyDataTypeMapping("NUMBER(38,37)", Decimal.schema(37));
    verifyDataTypeMapping("DATE", Date.SCHEMA);
    verifyDataTypeMapping("TIME", Time.SCHEMA);
    verifyDataTypeMapping("TIMESTAMP_LTZ", Timestamp.SCHEMA);
    verifyDataTypeMapping("BINARY", Schema.BYTES_SCHEMA);
  }

  @Test
  public void createOneColNoPk() {
    verifyCreateOneColNoPk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"col1\" NUMBER NOT NULL)");
  }

  @Test
  public void createOneColOnePk() {
    verifyCreateOneColOnePk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" NUMBER NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\"))");
  }

  @Test
  public void createThreeColTwoPk() {
    verifyCreateThreeColTwoPk(
        "CREATE TABLE \"test\" (" + System.lineSeparator() +
        "\"pk1\" NUMBER NOT NULL," + System.lineSeparator() +
        "\"pk2\" NUMBER NOT NULL," + System.lineSeparator() +
        "\"col1\" NUMBER NOT NULL," + System.lineSeparator() +
        "PRIMARY KEY(\"pk1\",\"pk2\"))"
    );
  }

  @Test
  public void alterAddOneCol() {
    verifyAlterAddOneCol(
        "ALTER TABLE \"test\" ADD \"newcol1\" NUMBER NULL"
    );
  }

  @Test
  public void alterAddTwoCol() {
    verifyAlterAddTwoCols(
        "ALTER TABLE \"test\" " + System.lineSeparator()
        + "ADD \"newcol1\" NUMBER NULL," + System.lineSeparator()
        + "ADD \"newcol2\" NUMBER DEFAULT 42"
    );
  }

  @Test
  public void upsert1() {
    assertEquals(
            "merge into \"Customer\" AS target using (select ? AS \"id\", ? AS \"name\", ? AS \"salary\", ? AS \"address\") "
                    + "AS incoming on (target.\"id\"=incoming.\"id\") when matched then update set "
                    + "\"name\"=incoming.\"name\",\"salary\"=incoming.\"salary\",\"address\"=incoming.\"address\" when not matched then insert "
                    + "(\"name\", \"salary\", \"address\", \"id\") values (incoming.\"name\",incoming.\"salary\",incoming.\"address\",incoming.\"id\");",
            dialect.getUpsertQuery("Customer", Collections.singletonList("id"), Arrays.asList("name", "salary", "address"))
    );
  }

  @Test
  public void upsert2() {
    assertEquals(
            "merge into \"Book\" AS target using (select ? AS \"author\", ? AS \"title\", ? AS \"ISBN\", ? AS \"year\", ? AS \"pages\")"
                    + " AS incoming on (target.\"author\"=incoming.\"author\" and target.\"title\"=incoming.\"title\")"
                    + " when matched then update set \"ISBN\"=incoming.\"ISBN\",\"year\"=incoming.\"year\",\"pages\"=incoming.\"pages\" when not "
                    + "matched then insert (\"ISBN\", \"year\", \"pages\", \"author\", \"title\") values (incoming.\"ISBN\",incoming.\"year\","
                    + "incoming.\"pages\",incoming.\"author\",incoming.\"title\");",
            dialect.getUpsertQuery("Book", Arrays.asList("author", "title"), Arrays.asList("ISBN", "year", "pages"))
    );
  }
}
