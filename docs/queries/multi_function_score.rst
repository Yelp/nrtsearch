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
            }
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