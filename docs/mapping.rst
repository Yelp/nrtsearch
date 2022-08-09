Mapping
==========================

Mapping defines how a document and its fields are stored and indexed. In NRTSearch, every index are required to have a mapping for it documents.

Field Data Types
---------------------------

Every field has a field data type which indicates type of data the field contains.

Common types
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* BOOLEAN
Basic boolean data types

* Numbers
Support different number types like INT, LONG, FLOAT, DOUBLE

* ATOM
ATOM is used for structured content like email addresses, hostnames. It is used for exact match. No analyzers supported.

* DATE_TIME
Field data type to store the datetime.

* _ID
This field would be used for the primary key of a document like business id or user id. _ID field type is stored as string.


Text search types
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* TEXT
The tradition field for full-text content. Text fields are best suited for unstructured but human-readable content. Content of this field can be tokenized and analyzed.

Object types
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* OBJECT
Object field is used to store Json data. Object fields are not searchable. But it is possible to search the inner fields of object.

Spatial data types
^^^^^^^^^^^^^^^^^^^^^^^^^^^

* LAT_LON
LAT_LON field is used for geo point field like [latitude, longitude].

* POLYGON
POLYGON field is used for closed geo polygons with a list of geo points. The first and last geo points must the the same as a closed loop polygon.

Mapping Parameters
---------------------------

* multiValued
This parameter indicates whether this field should be a single value or array of values.

* search
This parameter indicates whether this field is indexed so that certain types of queries can be applied to this field.

* storeDocValues
This parameter indicates whether this field is stored as doc value so that we can retrieve this field in the NRTSearch response.

* tokenize
This parameter is only for text. This parameter indicates whether this text field should be tokenized.

* indexAnalyzer
This parameter is only for text usually together with tokenize. This parameter specifies the analyzer to use for this field during indexing.

* searchAnalyzer
This parameter is only for text usually together with tokenize. This parameter specifies the analyzer to use for this field during searching.
