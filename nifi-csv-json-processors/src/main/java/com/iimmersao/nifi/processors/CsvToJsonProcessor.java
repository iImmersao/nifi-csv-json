package com.iimmersao.nifi.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"csv", "json", "convert"})
@CapabilityDescription("Converts CSV content into JSON format")
public class CsvToJsonProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success").description("Successful conversion").build();

    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure").description("Conversion failed").build();

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships = Set.of(REL_SUCCESS, REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session)
            throws ProcessException {

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            List<String[]> rows = new ArrayList<>();

            session.read(flowFile, (InputStreamCallback) in -> {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(in, StandardCharsets.UTF_8));
                reader.lines().forEach(line -> rows.add(line.split(",")));
            });

            String[] headers = rows.remove(0);
            ArrayNode arrayNode = mapper.createArrayNode();

            for (String[] row : rows) {
                ObjectNode obj = mapper.createObjectNode();
                for (int i = 0; i < headers.length && i < row.length; i++) {
                    obj.put(headers[i], row[i]);
                }
                arrayNode.add(obj);
            }

            FlowFile output = session.write(flowFile, (OutputStreamCallback) out ->
                    out.write(mapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsBytes(arrayNode)));

            session.transfer(output, REL_SUCCESS);

        } catch (Exception e) {
            getLogger().error("CSV to JSON conversion failed", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
