Spean Multi Term Query 
==========================

public class SpanMultiTermQuery<Q extends MultiTermQuery>
extends SpanQuery
Wraps any MultiTermQuery as a SpanQuery, so it can be nested within other SpanQuery classes.
The query is rewritten by default to a SpanOrQuery containing the expanded terms, but this can be customized.

Example:

 WildcardQuery wildcard = new WildcardQuery(new Term("field", "bro?n"));
 SpanQuery spanWildcard = new SpanMultiTermQuery<WildcardQuery>(wildcard);
 // do something with spanWildcard, such as use it in a SpanFirstQuery


Proto definition:

.. code-block::

    // Wrapper message for different types of SpanQuery
    message SpanQuery {
      oneof query {
        TermQuery spanTermQuery = 1;
        SpanNearQuery spanNearQuery = 2;
        SpanMultiTermQueryspanMultiTermQuery= 3;
      }
    }

    message Term {
        string field = 1;
        string text = 2;
    }

    // A query that matches documents containing terms matching a pattern.
    message WildcardQuery {
        Term term = 1;
    }

    // A query that matches documents containing terms similar to the specified term.
    message FuzzyQuery {
        Term term = 1;
        // The maximum allowed Levenshtein Edit Distance (or number of edits). Possible values are 0, 1 and 2. Either set this or auto. Default is 2.
        int32 maxEdits = 2;
        // Length of common (non-fuzzy) prefix. Default is 0.
        int32 prefixLength = 3;
        // The maximum number of terms to match. Default is 50.
        int32 maxExpansions = 4;
        // True if transpositions should be treated as a primitive edit operation. If this is false, comparisons will implement the classic Levenshtein algorithm. Default is true.
        bool transpositions = 5;
    }

    // A query that matches documents that contain a specific prefix in a provided field.
    message PrefixQuery {
        // Document field name.
        string field = 1;
        // Prefix to search for.
        string prefix = 2;
        // Method used to rewrite the query.
        RewriteMethod rewrite = 3;
        // Specifies the size to use for the TOP_TERMS* rewrite methods.
        int32 rewriteTopTermsSize = 4;
    }

    // Enum for RegexpQuery flags
    enum RegexpFlag {
        // Syntax flag, enables all optional regexp syntax.
        REGEXP_ALL = 0;
        // Syntax flag, enables anystring (@).
        REGEXP_ANYSTRING = 1;
        // Syntax flag, enables named automata (<identifier>).
        REGEXP_AUTOMATON = 2;
        // Syntax flag, enables complement (~).
        REGEXP_COMPLEMENT = 3;
        // Syntax flag, enables empty language (#).
        REGEXP_EMPTY = 4;
        // Syntax flag, enables intersection (&).
        REGEXP_INTERSECTION = 5;
        // Syntax flag, enables numerical intervals ( <n-m>).
        REGEXP_INTERVAL = 6;
        // Syntax flag, enables no optional regexp syntax.
        REGEXP_NONE = 7;
    }

    // Message for RegexpQuery
    message RegexpQuery {
        Term term = 1;
        // Optional flags for the regular expression
        RegexpFlag flag = 2;
        // maximum number of states that compiling the automaton for the regexp can result in. Set higher to allow more complex queries and lower to prevent memory exhaustion.
        int32 maxDeterminizedStates = 3;
    }

    // Message for a SpanMultiTermQuery
    message SpanMultiTermQuery{
      // The query to be wrapped
      oneof wrappedQuery {
          WildcardQuery wildcardQuery = 1;
          FuzzyQuery fuzzyQuery = 2;
          PrefixQuery prefixQuery = 3;
          RegexpQuery regexpQuery = 4;
      }
    }
