package com.yelp.nrtsearch.server.luceneserver.field;

import com.google.gson.Gson;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import org.apache.lucene.document.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ObjectFieldDef extends IndexableFieldDef{

    private final Gson gson;

    protected ObjectFieldDef(String name, Field requestField) {
        super(name, requestField);
        gson = new Gson();
    }

    @Override
    public String getType() {
        return "object";
    }

    @Override
    public void parseDocumentField(Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
    }

    @Override
    public void parseFieldWithChildren(
            Document document, List<String> fieldValues, List<List<String>> facetHierarchyPaths) {
        List<Map<String, Object>> fieldValueMaps = fieldValues
                .stream().map(e -> gson.fromJson(e, Map.class)).collect(Collectors.toList());
        parseFieldWithChildrenObject(document, fieldValueMaps, facetHierarchyPaths);
    }

    public void parseFieldWithChildrenObject(
            Document document, List<Map<String, Object>> fieldValues, List<List<String>> facetHierarchyPaths
    ) {
        for (Map.Entry<String, IndexableFieldDef> childField: childFields.entrySet()) {
            String[] keys = childField.getKey().split(IndexState.CHILD_FIELD_SEPARATOR);
            String key = keys[keys.length - 1];
            if(childField.getValue().getType().equals("object")) {
                List<Map<String, Object>> childrenValues = new ArrayList<>();
                for (Map<String, Object> fieldValue: fieldValues) {
                    Object childValue = fieldValue.get(key);
                    if (childValue != null) {
                        if (childValue instanceof Map) {
                            childrenValues.add((Map<String, Object>) childValue);
                        } else if (childValue instanceof List) {
                            childrenValues.addAll((List<Map<String, Object>>) childValue);
                        } else {
                            throw new IllegalArgumentException("Invalid data");
                        }
                    }
                }
                ((ObjectFieldDef) childField.getValue()).parseFieldWithChildrenObject(document, childrenValues, facetHierarchyPaths);
            } else {
                List<String> childrenValues = new ArrayList<>();
                for (Map<String, Object> fieldValue: fieldValues) {
                    Object childValue = fieldValue.get(key);
                    if (childValue != null) {
                        if (childValue instanceof List) {
                            ((List<Object>) childValue).stream().forEach(e -> childrenValues.add(String.valueOf(e)));
                        } else {
                            childrenValues.add(String.valueOf(childValue));
                        }
                    }
                }
                childField.getValue().parseFieldWithChildren(document, childrenValues, facetHierarchyPaths);
            }
        }
    }
}
