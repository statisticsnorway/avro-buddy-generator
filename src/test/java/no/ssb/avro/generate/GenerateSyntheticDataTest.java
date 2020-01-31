package no.ssb.avro.generate;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

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

    static class TestFieldChildHandler implements GenerateSyntheticData.FieldChildHandler {

        @Override
        public String field(Schema.Type type, String field, String value, int rowNum, int arrayElementNum) {
            if (field.equals("sex")) {
                return "Male";
            }
            if (field.equals("age")) {
                return Integer.toString(rowNum * 10 + arrayElementNum);
            }
            return value;
        }

        @Override
        public int getChildCount(int rowNum) {
            return 2;
        }

        @Override
        public int onNewRow(int rowNum) {
            return rowNum;
        }
    }

    TestFieldChildHandler fieldChildGenerator = new TestFieldChildHandler();

    @Test
    void testOverrideChangeOfFields() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 2, fieldChildGenerator);

        List<String> result = new ArrayList<>();
        for (DataElement element : generateSyntheticData) {
            GenericRecord genericRecord = SchemaAwareElement.toRecord(element, generateSyntheticData.getSchemaBuddy());
            result.add(genericRecord.toString());
        }
        assertThat(result).isEqualTo(List.of(
                "{\"id\": \"id_1_0\", \"person\": [{\"name\": \"name_1_0\", \"sex\": \"Male\", \"age\": 10}, {\"name\": \"name_1_1\", \"sex\": \"Male\", \"age\": 11}]}",
                "{\"id\": \"id_2_0\", \"person\": [{\"name\": \"name_2_0\", \"sex\": \"Male\", \"age\": 20}, {\"name\": \"name_2_1\", \"sex\": \"Male\", \"age\": 21}]}"
        ));
    }
}