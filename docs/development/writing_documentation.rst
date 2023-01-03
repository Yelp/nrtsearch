Writing Documentation
=====================

* Create a ``.rst`` file under ``<root>/docs/<subdirectory>``
* Add the content following `ReStructuredText <https://en.wikipedia.org/wiki/ReStructuredText>`_ format
* Make sure the new file can be accessed from the index by adding any links if needed
* Generate the documents to preview by running ``./gradlew site``
* The previews will be generated under ``<root>/build/docs``
* Make any changes if needed, once satisfied create a PR and merge to master after approval