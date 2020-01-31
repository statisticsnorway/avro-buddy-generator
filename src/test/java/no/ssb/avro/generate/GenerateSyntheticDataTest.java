package no.ssb.avro.generate;

import no.ssb.avro.convert.core.DataElement;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

class GenerateSyntheticDataTest {

    FieldChildGenerator fieldChildGenerator = new FieldChildGenerator();

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

    @Test
    void testFieldHandler() {
        GenerateSyntheticData generateSyntheticData = new GenerateSyntheticData(schema, 2, fieldChildGenerator);
        DataElement element = generateSyntheticData.generate();

        System.out.println(element.toString(true));
    }
}