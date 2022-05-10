Function Score Query
==========================

A query that wraps another query and uses custom scoring logic to compute the wrapped query's score. This uses FunctionScoreQuery in Lucene.

Proto definition:

.. code-block::

   message FunctionScoreQuery {
       Query query = 1; // Input query
       Script script = 2; // script definition to compute a custom document score
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