package no.ssb.avro.generate;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaBuddy;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class GenerateSyntheticDataTest {

    Schema schema = SchemaBuilder
            .record("root").namespace("no.ssb.data")
            .fields()
            .name("id").type().stringType().noDefault()
            .name("person").type().optional().type(
                    SchemaBuilder.array()
                            .items(SchemaBuilder.record("person")
                                    .fields()
                                    .name("name").type().stringType().noDefault()
                                    .name("sex").type().optional().stringType()
                                    .name("age").type().optional().intType()
                                    .endRecord()
                            )
            )
            .endRecord();

    static class TestInterceptor extends FieldInterceptor {
        @Override
        protected GeneratedField handleField(SchemaBuddy schema, int rowNum, int arrayElementNum) {
            String name = schema.getName();
            if (name.equals("sex")) {
                if (arrayElementNum == 0) return GeneratedField.missingStatus();
                if (arrayElementNum == 1) return GeneratedField.nullStatus();
                if (arrayElementNum == 2) return GeneratedField.shouldBeGenerated();
                return new GeneratedField("Male");
            }
            if (name.equals("age")) {
                return GeneratedField.fromInteger(rowNum * 10 + arrayElementNum);
            }
            return GeneratedField.shouldBeGenerated();
        }

        @Override
        public int getChildCount(int rowNum) {
            return 4;
        }

        @Override
        public boolean skipRecord(SchemaBuddy schema, int rowNum, int level) {
            return false;
        }
    }

    TestInterceptor fieldChildGenerator = new TestInterceptor();

    @Test
    void testOverrideChangeOfFields() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 2, fieldChildGenerator);

        StringBuilder sb = new StringBuilder();
        for (DataElement element : generateSyntheticData) {
            sb.append(element.toString(true)).append("\n");
        }
        assertThat(sb.toString()).isEqualTo("root value:null\n" +
                " |-- id value:id_1_0\n" +
                " |-- person value:null\n" +
                " |    |-- name value:name_1_0\n" +
                " |    |-- age value:10\n" +
                " |-- person value:null\n" +
                " |    |-- name value:name_1_1\n" +
                " |    |-- sex value:null\n" +
                " |    |-- age value:11\n" +
                " |-- person value:null\n" +
                " |    |-- name value:name_1_2\n" +
                " |    |-- sex value:sex_1_2\n" +
                " |    |-- age value:12\n" +
                " |-- person value:null\n" +
                " |    |-- name value:name_1_3\n" +
                " |    |-- sex value:Male\n" +
                " |    |-- age value:13\n" +
                "\n" +
                "root value:null\n" +
                " |-- id value:id_2_0\n" +
                " |-- person value:null\n" +
                " |    |-- name value:name_2_0\n" +
                " |    |-- age value:20\n" +
                " |-- person value:null\n" +
                " |    |-- name value:name_2_1\n" +
                " |    |-- sex value:null\n" +
                " |    |-- age value:21\n" +
                " |-- person value:null\n" +
                " |    |-- name value:name_2_2\n" +
                " |    |-- sex value:sex_2_2\n" +
                " |    |-- age value:22\n" +
                " |-- person value:null\n" +
                " |    |-- name value:name_2_3\n" +
                " |    |-- sex value:Male\n" +
                " |    |-- age value:23\n\n");

    }
}