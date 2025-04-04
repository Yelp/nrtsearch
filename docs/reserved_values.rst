Reserved Values
=================
This section contains values that are reserved for internal use. These values should be avoided to prevent
unexpected behavior. The reserved values are:

- **%NULL_SORT_VALUE%**: Used to denote a null value for a sort field in LastHitInfo. Use of this value in your documents may cause queries that use searchAfter to behave incorrectly.