Adding A New Live Setting
==========================

* Add new live setting to ``IndexSettings`` in ``luceneserver.proto`` and regenerate the protobuf files
* Add abstract getter method to ``IndexState.java``
* Implement the getter method in ``LegacyIndexState.java`` and return a sane default
* In ``ImmutableIndexState.java``:
   * Create a class field to hold the instance value
   * Assign instance value in the constructor
   * Implement the getter method and return the class field
   * Add default value in ``DEFAULT_INDEX_LIVE_SETTINGS`` which is used when the field is not specified in the committed state
   * Add validation to ``validateLiveSettings`` method
* Add to ``LiveSettingsV2Command`` in cli
* Access the new live setting by calling ``indexStateManager.getCurrent().get<new_live_setting_name>()``
* Add _set/_default/_invalid tests for the new property to ``ImmutableIndexStateTest.java``
