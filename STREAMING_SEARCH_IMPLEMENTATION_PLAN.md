# Streaming Search gRPC Endpoint - Implementation Plan

## Overview
Add a new streaming gRPC endpoint that returns search results as a stream, where:
- **First message**: Contains all response metadata (diagnostics, facets, total hits, search state, profile results, etc.) but NO hits
- **Subsequent messages**: Each message contains a single hit (or batch of hits)

## Quick Reference

### Key Naming Conventions
- **Proto field names**: Use `snake_case` (e.g., `hit_timeout`, `total_hits`, `facet_result`)
- **Proto message names**: Use `CamelCase` (e.g., `StreamingSearchResponse`, `StreamingSearchResponseHeader`)
- **RPC method name**: `streamingSearch` (generates Java method `streamingSearch()`)

### Build Command
After making proto changes, run:
```bash
./gradlew clean installDist
```

### New RPC Endpoint
```protobuf
rpc streamingSearch (SearchRequest) returns (stream StreamingSearchResponse)
```

### Batch Size Configuration
- **Default**: 50 hits per message
- **Configurable via**: `SearchRequest.streaming_hit_batch_size` field (int32)
- **Behavior**: 0 or negative values use default (50)

### Proto Changes Summary
1. `clientlib/src/main/proto/yelp/nrtsearch/search.proto`:
   - Add 3 new message types: `StreamingSearchResponseHeader`, `StreamingSearchResponseHits`, `StreamingSearchResponse`
   - Add 1 new field to `SearchRequest`: `streaming_hit_batch_size` (field number 32)
2. `clientlib/src/main/proto/yelp/nrtsearch/luceneserver.proto`:
   - Add 1 RPC endpoint: `streamingSearch`

## Architecture Analysis

### Current Search Implementation
- **Endpoint**: `rpc search (SearchRequest) returns (SearchResponse)`
- **Handler**: `SearchHandler.java` (lines 82-1233)
- **Proto Definition**:
  - Request: `SearchRequest` in `search.proto` (lines 711-788)
  - Response: `SearchResponse` in `search.proto` (lines 954-1090)

### Key Components
1. **Search Flow**:
   - Request processing → Query execution → Hit collection → Field fetching → Response building
   - All processing happens in `SearchHandler.handle()` method (lines 144-376)

2. **Response Structure** (`SearchResponse`):
   - `Diagnostics diagnostics` - timing and debug info
   - `bool hitTimeout` - timeout indicator
   - `TotalHits totalHits` - total hit count
   - `repeated Hit hits` - **THE PART WE NEED TO STREAM**
   - `SearchState searchState` - state for pagination
   - `repeated FacetResult facetResult` - facet results
   - `ProfileResult profileResult` - profiling data
   - `map<string, CollectorResult> collectorResults` - collector results
   - `bool terminatedEarly` - early termination flag

## Implementation Plan

### Phase 1: Proto Definition Changes

**IMPORTANT**: Follow protobuf naming conventions used in this codebase:
- Message names: `CamelCase` (e.g., `StreamingSearchResponse`)
- Field names: `snake_case` (e.g., `hit_timeout`, `total_hits`)
- Oneof names: `CamelCase` (e.g., `ResponseType`)

#### 1.1 Create New Streaming Messages
**File**: `clientlib/src/main/proto/yelp/nrtsearch/search.proto`

Add new message types at the end of the file (after line 1503, after the `KnnQuery` message):

```protobuf
// Streaming search response - first message in stream
message StreamingSearchResponseHeader {
    // Query diagnostics
    SearchResponse.Diagnostics diagnostics = 1;
    // Set to true if search times out and a degraded response is returned
    bool hit_timeout = 2;
    // Total hits for the query
    TotalHits total_hits = 3;
    // State for use in subsequent searches (search after)
    SearchResponse.SearchState search_state = 4;
    // Counts or aggregates for a single dimension
    repeated FacetResult facet_result = 5;
    // Detailed stats returned when profile=true in request
    ProfileResult profile_result = 6;
    // Results from any additional document collectors
    map<string, CollectorResult> collector_results = 7;
    // If this query hit the terminateAfter threshold specified in the request
    bool terminated_early = 8;
}

// Streaming search response - hit chunk message
message StreamingSearchResponseHits {
    // One or more hits (could batch for efficiency)
    repeated SearchResponse.Hit hits = 1;
}

// Wrapper for streaming search response
message StreamingSearchResponse {
    oneof ResponseType {
        // First message: contains everything except hits
        StreamingSearchResponseHeader header = 1;
        // Subsequent messages: contain hits
        StreamingSearchResponseHits hits = 2;
    }
}
```

#### 1.2 Add Batch Size Parameter to SearchRequest
**File**: `clientlib/src/main/proto/yelp/nrtsearch/search.proto`

Add a new field to the `SearchRequest` message (after line 787, after the `knn` field):

```protobuf
    // Batch size for streaming search responses. When using the streamingSearch RPC, this controls how many
    // hits are sent in each streamed message after the initial header. Default is 50 if not specified or set to 0.
    // This parameter is ignored for non-streaming search endpoints.
    int32 streamingHitBatchSize = 32;
```

#### 1.3 Add New RPC Endpoint
**File**: `clientlib/src/main/proto/yelp/nrtsearch/luceneserver.proto`

After line 178 (after the `searchV2` RPC endpoint), add:

```protobuf
    // Execute a search query against an index, streaming results back. The first message in the stream
    // contains all response metadata (diagnostics, facets, total hits, etc.) except hits. Subsequent messages
    // contain batches of hits. The batch size can be controlled via SearchRequest.streaming_hit_batch_size.
    rpc streamingSearch (SearchRequest) returns (stream StreamingSearchResponse) {
        option (google.api.http) = {
            post: "/v1/streaming_search"
            body: "*"
        };
    }
```

**Note**: The RPC method is named `streamingSearch` (camelCase following gRPC conventions) which will generate the Java method name `streamingSearch()`.

### Phase 2: Handler Implementation

#### 2.1 Create StreamingSearchHandler
**File**: `src/main/java/com/yelp/nrtsearch/server/handler/StreamingSearchHandler.java`

Create a new handler class that extends the base `Handler` class:

```java
public class StreamingSearchHandler extends Handler<SearchRequest, StreamingSearchResponse> {
    private final SearchHandler searchHandler;

    public StreamingSearchHandler(GlobalState globalState) {
        super(globalState);
        this.searchHandler = new SearchHandler(globalState);
    }

    @Override
    public void handle(SearchRequest searchRequest,
                      StreamObserver<StreamingSearchResponse> responseObserver) {
        // Main streaming logic here
    }
}
```

Key implementation details:

1. **Reuse SearchHandler logic**: Most of the search execution logic from `SearchHandler` can be reused
2. **Modify response building**: Instead of building a single `SearchResponse`, we need to:
   - Build the header message with all metadata
   - Stream hits individually or in small batches

3. **Streaming approach**:
   ```java
   // 1. Execute search (reuse SearchHandler logic up to hit fetching)
   SearchContext searchContext = buildSearchContext(...);
   TopDocs hits = executeSearch(searchContext);

   // 2. Send header first
   StreamingSearchResponse header = buildHeaderResponse(searchContext);
   responseObserver.onNext(header);

   // 3. Get batch size from request (default to 50 if not specified or 0)
   int batchSize = searchRequest.getStreamingHitBatchSize();
   if (batchSize <= 0) {
       batchSize = 50; // default batch size
   }

   // 4. Fetch and stream hits in batches
   List<SearchResponse.Hit> batch = new ArrayList<>(batchSize);
   for (ScoreDoc hit : hits.scoreDocs) {
       SearchResponse.Hit hitMessage = fetchAndBuildHit(hit, searchContext);
       batch.add(hitMessage);

       // Send batch when full
       if (batch.size() >= batchSize) {
           StreamingSearchResponse hitResponse = StreamingSearchResponse.newBuilder()
               .setHits(StreamingSearchResponseHits.newBuilder().addAllHits(batch))
               .build();
           responseObserver.onNext(hitResponse);
           batch.clear();
       }
   }

   // 5. Send any remaining hits in partial batch
   if (!batch.isEmpty()) {
       StreamingSearchResponse hitResponse = StreamingSearchResponse.newBuilder()
           .setHits(StreamingSearchResponseHits.newBuilder().addAllHits(batch))
           .build();
       responseObserver.onNext(hitResponse);
   }

   // 6. Complete stream
   responseObserver.onCompleted();
   ```

#### 2.2 Refactor SearchHandler for Reusability
**File**: `src/main/java/com/yelp/nrtsearch/server/handler/SearchHandler.java`

Extract reusable methods (if needed):
- Query execution logic (already in `handle()` method)
- Hit fetching logic (already in `fetchFields()` method at line 388)
- Consider making helper methods protected or package-private for reuse

### Phase 3: Server Registration

#### 3.1 Register Streaming Endpoint
**File**: `src/main/java/com/yelp/nrtsearch/server/grpc/LuceneServer.java`

Add the streaming search handler to the server:

```java
@Override
public void streamingSearch(SearchRequest request,
                           StreamObserver<StreamingSearchResponse> responseObserver) {
    StreamingSearchHandler handler = new StreamingSearchHandler(globalState);
    handler.handle(request, responseObserver);
}
```

### Phase 4: Implementation Details

#### 4.1 Hit Batching Strategy
**Selected: Option B - Batch streaming with configurable batch size**

Hits will be streamed in batches rather than one at a time:
- **Default batch size**: 50 hits per message
- **Configurable**: Via `SearchRequest.streaming_hit_batch_size` field
- **Benefits**:
  - Reduces gRPC message overhead
  - Still provides streaming benefits
  - Better network efficiency
  - Tunable based on use case

**Implementation notes**:
- Batch size of 0 or negative values will use the default (50)
- Last batch may be partial if total hits is not a multiple of batch size
- Batch size is ignored for non-streaming search endpoints

#### 4.2 Error Handling
- Errors before sending header: Send error through `responseObserver.onError()`
- Errors after sending header:
  - Log the error
  - Send remaining hits if possible
  - Complete stream with `onCompleted()` or `onError()` depending on severity

#### 4.3 Timeout Handling
- Current timeout logic is in `SearchCutoffWrapper` (referenced in SearchHandler)
- For streaming: If timeout occurs mid-stream, send what's available so far
- Set `hitTimeout=true` in the header if timeout occurred

#### 4.4 Parallel Fetch Considerations
The current implementation supports parallel field fetching (lines 410-513 in SearchHandler):
- `parallelFetchByField`: Fetch fields in parallel
- `parallelFetchByDoc`: Fetch documents in parallel

For streaming, we need to adapt this:
- Fetch a batch of hits in parallel
- Stream them as soon as they're ready
- Continue to next batch

### Phase 5: Testing

#### 5.1 Unit Tests
Create test file: `src/test/java/com/yelp/nrtsearch/server/handler/StreamingSearchHandlerTest.java`

Test cases:
1. **Basic streaming**: Verify header is sent first, then hits
2. **Empty results**: Verify header is sent with zero hits
3. **Large result set**: Verify all hits are streamed correctly
4. **Timeout handling**: Verify partial results are streamed on timeout
5. **Error handling**: Verify errors are handled gracefully
6. **Facets and collectors**: Verify they appear in header, not with hits
7. **Field fetching**: Verify all requested fields are included in hits
8. **Default batch size**: Verify batch size defaults to 50 when not specified
9. **Custom batch size**: Verify custom batch sizes are respected (test with 10, 25, 100)
10. **Zero/negative batch size**: Verify 0 or negative values default to 50
11. **Batch boundaries**: Verify correct batching when total hits is not a multiple of batch size
12. **Single hit**: Verify single hit is sent in one batch

#### 5.2 Integration Tests
Create test file: `src/test/java/com/yelp/nrtsearch/server/grpc/StreamingSearchIntegrationTest.java`

Test cases:
1. **End-to-end streaming**: Full streaming with actual index
2. **Result equivalence**: Compare streaming vs non-streaming results (should be identical)
3. **Performance comparison**: Measure time to first message vs total time
4. **Client-side consumption**: Test different client consumption patterns
5. **Batch size variations**: Test with various batch sizes (1, 10, 50, 100, 1000)
6. **Large result sets**: Test with 10K+ hits to verify memory efficiency
7. **Concurrent streams**: Test multiple concurrent streaming requests
8. **Batch size parameter ignored in regular search**: Verify non-streaming search ignores the parameter

### Phase 6: Performance Considerations

#### 6.1 Memory Optimization
- **Current behavior**: Builds all hits in memory before returning
- **Streaming behavior**: Can release memory for each hit after streaming
- **Benefit**: Lower peak memory usage for large result sets

#### 6.2 Time to First Result
- **Current behavior**: User waits for entire search to complete
- **Streaming behavior**: User receives header immediately after search phase
- **Benefit**: Faster perceived response time

#### 6.3 Backpressure Handling
- gRPC handles backpressure automatically
- If client is slow to consume, server will slow down streaming
- Monitor for potential blocking in field fetching

### Phase 7: Client Library Updates

#### 7.1 Update Generated Code
After proto changes, regenerate client code and rebuild:
```bash
./gradlew clean installDist
```

This command will:
- Clean previous builds
- Regenerate Java code from proto files
- Compile the project
- Create distribution files

#### 7.2 Client Example Usage
Provide example in documentation:

```java
// Java client example
SearchRequest request = SearchRequest.newBuilder()
    .setIndexName("my_index")
    .setQuery(query)
    .setTopHits(1000)
    .setStreamingHitBatchSize(50)  // Optional: defaults to 50
    .build();

Iterator<StreamingSearchResponse> responses =
    blockingStub.streamingSearch(request);

// First message - header
StreamingSearchResponse firstMsg = responses.next();
StreamingSearchResponseHeader header = firstMsg.getHeader();
System.out.println("Total hits: " + header.getTotalHits().getValue());

// Subsequent messages - batches of hits
int hitCount = 0;
while (responses.hasNext()) {
    StreamingSearchResponse msg = responses.next();
    for (SearchResponse.Hit hit : msg.getHits().getHitsList()) {
        // Process hit
        hitCount++;
        System.out.println("Hit " + hitCount + ": " + hit);
    }
}
System.out.println("Received " + hitCount + " total hits");
```

### Phase 8: Documentation

#### 8.1 API Documentation
Update API docs to include:
- New streaming endpoint description
- Use cases for streaming vs. non-streaming
- Performance characteristics
- Example usage in different languages

#### 8.2 Migration Guide
Document for users:
- When to use streaming vs. standard search
- Breaking changes (none expected, this is a new endpoint)
- Performance implications

## Implementation Order

1. **Proto changes** (Phase 1) - ~2 hours
   - Define new message types with correct naming conventions (snake_case for fields)
   - Add RPC endpoint
   - Run `./gradlew clean installDist` to regenerate code

2. **Basic handler implementation** (Phase 2) - ~1 day
   - Create StreamingSearchHandler
   - Implement basic streaming logic
   - Test with simple queries

3. **Advanced features** (Phase 4) - ~1 day
   - Hit batching
   - Error handling
   - Timeout handling
   - Integration with parallel fetch

4. **Testing** (Phase 5) - ~1 day
   - Unit tests
   - Integration tests
   - Performance tests

5. **Documentation** (Phase 8) - ~0.5 day
   - API docs
   - Examples
   - Migration guide

**Total Estimated Time**: 3-4 days

## Key Design Decisions

### 1. Why Separate Header and Hits Messages?
- **Flexibility**: Client can process metadata immediately
- **Memory efficiency**: Client can start processing hits before all are received
- **Clarity**: Clear separation of concerns in protocol

### 2. Why Keep SearchRequest Unchanged?
- **Backward compatibility**: Existing clients unaffected
- **Simplicity**: Same request format for both streaming and non-streaming
- **Gradual migration**: Users can switch between endpoints without request changes

### 3. Hit Batching Size
- **Default**: 50 hits per message
- **Configurable**: Via `SearchRequest.streaming_hit_batch_size` parameter
- **Rationale**: Balance between message overhead and real-time feedback
  - 50 hits is a good default for most use cases
  - Can be tuned lower (e.g., 10) for more real-time feedback
  - Can be tuned higher (e.g., 100-200) for better throughput on high-latency networks

### 4. Error Handling Strategy
- **Before header sent**: Fail entire request
- **After header sent**: Send partial results, log error
- **Rationale**: Matches streaming best practices, maximizes data delivery

## Alternative Approaches Considered

### Alternative 1: Stream Everything Individually
Instead of header + hits, stream each response component separately:
- Message 1: Diagnostics
- Message 2: Total hits
- Message 3: Facets
- Messages 4+: Individual hits

**Rejected because**:
- More complex protocol
- Client needs to handle many message types
- Not significantly better than header + hits approach

### Alternative 2: Use Existing SearchResponse with `repeated` Field
Just stream multiple `SearchResponse` messages, each with a subset of hits.

**Rejected because**:
- Wastes bandwidth by repeating metadata in each message
- Less clear protocol
- Harder to optimize

### Alternative 3: Bidirectional Streaming
Allow client to control pace of streaming.

**Rejected because**:
- Unnecessary complexity for this use case
- gRPC backpressure handles this automatically
- Can be added later if needed

## Risks and Mitigations

### Risk 1: Performance Regression
**Mitigation**:
- Keep existing search endpoint unchanged
- Extensive performance testing before release
- Make streaming opt-in

### Risk 2: Increased Server Resource Usage
**Mitigation**:
- Implement proper backpressure handling
- Monitor active streaming connections
- Consider connection limits

### Risk 3: Client Compatibility
**Mitigation**:
- New endpoint doesn't affect existing endpoints
- Provide clear documentation and examples
- Support both streaming and non-streaming indefinitely

## Success Metrics

1. **Functional**: Streaming endpoint returns identical results to standard search
2. **Performance**: Time to first byte < 50% of standard search total time
3. **Memory**: Peak memory usage for large queries < 70% of standard search
4. **Reliability**: Error rate < 0.01% for streaming endpoint

## Files to Create/Modify

### New Files:
1. `src/main/java/com/yelp/nrtsearch/server/handler/StreamingSearchHandler.java`
2. `src/test/java/com/yelp/nrtsearch/server/handler/StreamingSearchHandlerTest.java`
3. `src/test/java/com/yelp/nrtsearch/server/grpc/StreamingSearchIntegrationTest.java`

### Modified Files:
1. `clientlib/src/main/proto/yelp/nrtsearch/search.proto` - Add streaming message types + batch size field to SearchRequest
2. `clientlib/src/main/proto/yelp/nrtsearch/luceneserver.proto` - Add RPC endpoint
3. `src/main/java/com/yelp/nrtsearch/server/grpc/LuceneServer.java` - Register handler
4. Potentially: `src/main/java/com/yelp/nrtsearch/server/handler/SearchHandler.java` - Extract reusable methods (optional)

## Future Enhancements

1. ~~**Configurable batching**: Allow client to specify batch size in request~~ ✅ **IMPLEMENTED** via `streaming_hit_batch_size` parameter
2. **Compression**: Evaluate per-message compression options
3. **Progress updates**: Include progress percentage in messages
4. **Cancellation**: Support client-initiated cancellation
5. **Resume capability**: Support resuming interrupted streams
6. **Adaptive batching**: Automatically adjust batch size based on network conditions

## References

- gRPC streaming documentation: https://grpc.io/docs/what-is-grpc/core-concepts/#server-streaming-rpc
- Lucene search API: https://lucene.apache.org/core/
- Current nrtsearch search implementation: `src/main/java/com/yelp/nrtsearch/server/handler/SearchHandler.java`
