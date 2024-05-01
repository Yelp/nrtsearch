Multi Function Score Query
==========================

A query that wraps another query and uses a number of specified functions to modify the document scores.
Functions may specify a filter to apply to a subset of recalled documents. How the function scores
are combined, and how this final function score modifies the original document score are specified
as parameters.

Proto definition:

.. code-block::

    // A query to modify the score of documents with a given set of functions
    message MultiFunctionScoreQuery {
        // Function to produce a weighted value
        message FilterFunction {
            // Apply function only to docs that pass this filter, match all if not specified
            Query filter = 1;
            // Weight to multiply with function score, 1.0 if not specified
            float weight = 2;
            // Function to produce score, will be 1.0 if none are set
            oneof Function {
                // Produce score with score script definition
                Script script = 3;
                // Produce score with a decay function
                DecayFunction decayFunction = 4;
            }
        }

        // Apply decay function to docs
        message DecayFunction {
            // Document field name to use
            string fieldName = 1;
            // Type of decay function to apply
            DecayType decayType = 2;
            // Origin point to calculate the distance
            oneof Origin {
                google.type.LatLng geoPoint = 3;
            }
            // Currently only distance based scale and offset units are supported
            // Distance from origin + offset at which computed score will be equal to decay. Scale should be distance, unit (m, km, mi) with space is optional. Default unit will be meters. Ex: "10", 15 km", "5 m", "7 mi"
            string scale = 4;
            // Compute decay function for docs with a distance greater than offset, will be 0.0 if none is set. Offset should be distance, unit (m, km, mi) with space is optional. Default unit will be meters. Ex: "10", 15 km", "5 m", "7 mi"
            string offset = 5;
            // Defines decay rate for scoring. Should be between (0, 1)
            float decay = 6;
        }

        enum DecayType {
            // Exponential decay function
            DECAY_TYPE_EXPONENTIAL = 0;
            // Linear decay function
            DECAY_TYPE_LINEAR = 1;
            // Gaussian decay function
            DECAY_TYPE_GUASSIAN = 2;
        }


        // How to combine multiple function scores to produce a final function score
        enum FunctionScoreMode {
            // Multiply weighted function scores together
            SCORE_MODE_MULTIPLY = 0;
            // Add weighted function scores together
            SCORE_MODE_SUM = 1;
        }

        // How to combine final function score with query score
        enum BoostMode {
            // Multiply scores together
            BOOST_MODE_MULTIPLY = 0;
            // Add scores together
            BOOST_MODE_SUM = 1;
            // Ignore the query score, and use the function score only
            BOOST_MODE_REPLACE = 2;
        }

        // Main query to produce recalled docs and scores, which will be modified by the final function score
        Query query = 1;
        // Functions to produce final function score
        repeated FilterFunction functions = 2;
        // Method to combine functions scores
        FunctionScoreMode score_mode = 3;
        // Method to modify query document scores with final function score
        BoostMode boost_mode = 4;
        // Optional minimal score to match a document. By default, it's 0.
        float min_score = 5;
        // Determine minimal score is excluded or not. By default, it's false;
        bool min_excluded = 6;
    }
