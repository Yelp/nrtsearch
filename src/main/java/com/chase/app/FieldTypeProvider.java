package com.chase.app;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.joda.time.format.ISODateTimeFormat;

public class FieldTypeProvider {

    private static FieldType CustomizedText(IndexOptions options, Boolean withTermVectorPositions, Boolean withTermVectorOffsets)
    {
        FieldType f = new FieldType();
        f.setIndexOptions(options);
        f.setStoreTermVectors(withTermVectorOffsets || withTermVectorPositions);
        f.setStoreTermVectorOffsets(withTermVectorOffsets);
        f.setStoreTermVectorPositions(withTermVectorPositions);
        f.setTokenized(true);
        f.setStored(true);
        f.freeze();

        return f;
    }

    private static Field TextWithOffsets(String key, String value)
    {
        return
            new Field(key, value, 
                CustomizedText(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, true, true)
            );
    }

    public static Document AddField(String key, String value, Document doc) throws Exception { // TODO all text fields here have a default termvector and indexoptions different from ES. We should change this...
        if (value == null)  // TODO check what happens if we skip this test and index nulls
            return doc;
        switch (key) {
            case IndexedFieldsNames.ID:
            case IndexedFieldsNames.APP_ID:  // TODO how to set normalizer + split on white space in order to lookup on this?
            case IndexedFieldsNames.UPDATE_ID:
            case IndexedFieldsNames.ACCOUNT_ID:
            case IndexedFieldsNames.LINK_ID:
            case IndexedFieldsNames.EXTERNAL_ID:
                doc.add(new StringField(key, value, Field.Store.YES));
                break;
            case IndexedFieldsNames.TYPE: // TODO how to set normalizer + split on white space in order to lookup on this?
                doc.add(new StringField(key, value, Field.Store.YES));  // TODO change indexoptions for subfields
                doc.add(new TextField(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION), value, Field.Store.YES));
                doc.add(new TextField(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION), value, Field.Store.YES));
                doc.add(new TextField(String.format("%s.%s", key, IndexedFieldsExtensionsNames.PREFIXES), value, Field.Store.YES));
                //doc.add(new TextField(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LENGTH), value, Field.Store.YES)); // TODO removed
                break;
            case IndexedFieldsNames.FETCH_INPUT:
                FieldType ft =  new FieldType();
                ft.setStored(true);
                ft.setIndexOptions(IndexOptions.NONE);
                ft.setTokenized(false);
                doc.add(new Field(key, value, ft));
                break;
            case IndexedFieldsNames.CONTENT:
                doc.add(TextWithOffsets(key, value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.STEMMING), value));
                break;
            case IndexedFieldsNames.NAME:
                doc.add(TextWithOffsets(key, value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.SHINGLES), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.PREFIXES), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.STEMMING), value));

                doc.add(new TextField(String.format("%s.%s", key, IndexedFieldsExtensionsNames.KEYWORD), value, Field.Store.YES));
                break;
            case IndexedFieldsNames.TEXT:
                doc.add(TextWithOffsets(key, value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.SHINGLES), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.PREFIXES), value));
                doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.STEMMING), value));
                break;
   
            case IndexedFieldsNames.HIGHLIGHT:
                doc.add(TextWithOffsets(key, value));
                break;
           
            case IndexedFieldsNames.ANALYZED_TRAITS:
                //todo
                break;
            
            default:
                throw new Exception(String.format("Unsupported field key: %s", key));
            
        }

        // add facets
        switch(key) {
            case IndexedFieldsNames.APP_ID: 
            case IndexedFieldsNames.TYPE:
            case IndexedFieldsNames.LINK_ID:
                doc.add(new FacetField(key, value));
                break;
            default:
                break;
        }
        
        return doc;
    }
    private final static String[] _timestampTraits = new String[] {
        "sharedAt", "modifiedAt", "createdAt",  "start"
    };

    private static void AddAnalyzedTimestamp(LinkedHashMap<String, Object> traits, Document doc)
    {
        Optional<Object> timestampTrait = Arrays.stream(_timestampTraits).map(x -> traits.get(x)).filter(o -> o != null).findFirst();
        if (timestampTrait.isEmpty()){
            return;
        }

        long epoch = ISODateTimeFormat.dateTimeParser().parseDateTime(timestampTrait.get().toString()).toInstant().getMillis();
            
        doc.add(new NumericDocValuesField(IndexedFieldsNames.ANALYZED_TRAITS + ".timestamp", epoch));
    }

    public static Document AddField(String key, Object value, Document doc) throws Exception {
        switch (key) {
            case IndexedFieldsNames.DATA:
                try (ByteArrayOutputStream bos = new ByteArrayOutputStream()){
                    try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
                        out.writeObject(value);
                        out.flush();
                        doc.add(new StoredField(key, bos.toByteArray()));
                    }
                }
                break;
            case IndexedFieldsNames.TRAITS:
                if (value == null || !(value instanceof LinkedHashMap)) {
                    return doc;
                }
                LinkedHashMap<String, Object> lhm = (LinkedHashMap<String, Object>) value;
                for(String k : lhm.keySet()) {
                    Object v = lhm.get(k);
                    if (v instanceof String) {
                        doc.add(new FacetField(key, k, (String)v));
                    } else if (v instanceof ArrayList) {
                        for (String _v : (ArrayList<String>)v) {
                            doc.add(new FacetField(key, k, _v));
                        };
                    }
                }
                AddAnalyzedTimestamp(lhm, doc);
                break;
            default:
                throw new Exception(String.format("Unsupported field key: %s", key));
        }

        return doc;
    }

    public static Document AddField(String key, String[] value, Document doc) throws Exception {
        switch (key) {
            case IndexedFieldsNames.PATHS:
                for (String v : value) {
                    doc.add(TextWithOffsets(key, v));
                    doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION), v));
                    doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION_PREFIX), v));
                    doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION), v));
                    doc.add(TextWithOffsets(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX), v));
                }
                break;
            default:
                throw new Exception(String.format("Unsupported field key: %s", key));
        }

        return doc;
    }

    public static Document AddField(String key, Date value, Document doc) throws Exception {
        switch (key) {
            case IndexedFieldsNames.INDEX_TIME:
                doc.add(new StringField(key, DateTools.dateToString(value, Resolution.HOUR), Field.Store.YES));
                break;
            default:
                throw new Exception(String.format("Unsupported field key: %s", key));
        }

        return doc;
    }
}