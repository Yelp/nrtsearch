Date Time
=======
The field type is used to store date and time values.

.. code-block:: protobuf

    message Field {
        string name = 1;
        FieldType type = 2;
        bool search = 3;
        bool store = 4;
        bool storeDocValues = 5;
        bool multiValued = 9;
        string dateTimeFormat = 12;
    }

- **name**: Name of the field.
- **type**: Type of the field. Must be set to DATE_TIME.
- **search**: Whether the field should be indexed for search. Default is false.
- **store**: Whether the field should be stored in the index. Default is false.
- **storeDocValues**: Whether the field should be stored in doc values. Default is false.
- **multiValued**: Whether the field can contain more than one value. Default is false.
- **dateTimeFormat**: The format of the date time value. Accepts one of the following:
    - **epoch_millis**: Milliseconds since epoch
    - **strict_date_optional_time**: Date with optional time in pattern of "yyyy-MM-dd['T'HH:mm:ss[.SSS]]"
    - A `DateTimeFormatter <https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/time/format/DateTimeFormatter.html>`_ pattern string

Example
-------
.. code-block:: json

    {
        "name": "date_time_field",
        "type": "DATE_TIME",
        "search": true,
        "storeDocValues": true,
        "dateTimeFormat": "epoch_millis"
    }