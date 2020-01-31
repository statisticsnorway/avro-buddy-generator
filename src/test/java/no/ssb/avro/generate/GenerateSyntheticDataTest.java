package no.ssb.avro.generate;

import no.ssb.avro.convert.core.DataElement;
import no.ssb.avro.convert.core.SchemaAwareElement;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

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

    class TestFieldChildHandler implements GenerateSyntheticData.FieldChildHandler {

        @Override
        public String field(Schema.Type type, String field, String value, int rowNum) {
            if (field.equals("sex")) {
                return "Male";
            }
            if (field.equals("age")) {
                return Integer.toString(rowNum);
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
    void testFieldHandler() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 2, fieldChildGenerator);

        for (DataElement element : generateSyntheticData) {
//            System.out.println(element.toString(true));

            GenericRecord genericRecord = SchemaAwareElement.toRecord(element, generateSyntheticData.getSchemaBuddy());
            System.out.println(genericRecord);

        }
    }
}