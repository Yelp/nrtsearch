Function Filter Query
==========================

A query that retrieves all documents with a positive score calculated by the script.

Proto definition:

.. code-block::

   message FunctionFilterQuery {
       Script script = 1; // script definition to compute a custom document score
   }

   message Script {
       string lang = 1; // script language
       string source = 2; // script source

       // script parameter entry
       message ParamValue {
           oneof ParamValues {
               string textValue = 1;
               bool booleanValue = 2;
               int32 intValue = 3;
               int64 longValue = 4;
               float floatValue = 5;
               double doubleValue = 6;
               ParamNullValue nullValue = 7;
               ParamListValue listValue = 8;
               ParamStructValue structValue = 9;
           }
       }

       // null parameter value
       enum ParamNullValue {
           NULL_VALUE = 0;
       }

       // map parameter value
       message ParamStructValue {
           map<string, ParamValue> fields = 1;
       }

       // list parameter value
       message ParamListValue {
           repeated ParamValue values = 1;
       }

       map<string, ParamValue> params = 7; // parameters passed into script execution
   }
