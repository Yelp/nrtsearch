/*
 * Copyright 2020 Chase Labs Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chase.app.search;

// import java.io.Closeable;
// import java.io.IOException;
// import java.util.Arrays;
// import java.util.Collection;

// import com.chase.app.IndexedFieldsNames;
// import com.chase.app.Resource;
// import com.chase.app.search.contracts.ResponseItem;
// import com.chase.app.search.contracts.ResponseResourceItem;
// import com.chase.app.search.contracts.SearchFilter;
// import com.chase.app.search.contracts.SearchRequest;
// import com.chase.app.search.contracts.SearchResponse;

// import org.apache.lucene.document.Document;
// import org.apache.lucene.facet.DrillDownQuery;
// import org.apache.lucene.facet.DrillSideways;
// import org.apache.lucene.facet.FacetResult;
// import org.apache.lucene.facet.Facets;
// import org.apache.lucene.facet.FacetsCollector;
// import org.apache.lucene.facet.FacetsConfig;
// import org.apache.lucene.facet.DrillSideways.DrillSidewaysResult;
// import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
// import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
// import org.apache.lucene.index.DirectoryReader;
// import org.apache.lucene.index.IndexFileNames;
// import org.apache.lucene.index.IndexReader;
// import org.apache.lucene.index.MultiReader;
// import org.apache.lucene.search.Collector;
// import org.apache.lucene.search.IndexSearcher;
// import org.apache.lucene.search.MatchAllDocsQuery;
// import org.apache.lucene.search.Query;
// import org.apache.lucene.search.ScoreDoc;
// import org.apache.lucene.search.TopDocs;
// import org.apache.lucene.store.Directory;
// import org.elasticsearch.cluster.metadata.IndexAbstraction.Index;

// public class SearchProvider implements Closeable {
//     private IndexSearcher searcher;
//     private DirectoryTaxonomyReader localTaxoReader;
//     private DirectoryTaxonomyReader remoteTaxoReader;
//     private FacetsConfig facetsConfig;
//     private SearcherQueryBuilder queryBuilder;

//     public SearchProvider(Directory localIndex, Directory localTaxo, Directory remoteIndex, Directory remoteTaxo) throws IOException {
//         IndexReader reader = 
//             remoteIndex==null ? DirectoryReader.open(localIndex)
//                               : new MultiReader(DirectoryReader.open(localIndex),DirectoryReader.open(remoteIndex));
//         searcher = new IndexSearcher(reader);          
//         localTaxoReader = new DirectoryTaxonomyReader(localTaxo);
//         remoteTaxoReader = new DirectoryTaxonomyReader(remoteTaxo);
//         queryBuilder = new SearcherQueryBuilder();
//         InitFacets();
//     }

//     private void InitFacets() {
//         facetsConfig = new FacetsConfig();
//         facetsConfig.setIndexFieldName("traits", "facet_traits");
//         facetsConfig.setHierarchical("traits", true);
//         facetsConfig.setMultiValued("traits", true);
//     }


//     private void ApplyPostfilters(DrillDownQuery q, SearchFilter filters)
//     {
//         if (filters == null)
//             return;

//         if ( filters.appId != null)
//         {
//             for (String x : filters.appId) {
//                 q.add(IndexedFieldsNames.APP_ID, x);
//             }
//         }
//         if ( filters.resourceType != null)
//         {
//             for (String x : filters.resourceType) {
//                 q.add(IndexedFieldsNames.TYPE, x);
//             }
//         }
//         if ( filters.link != null)
//         {
//             for (String x : filters.link) {
//                 q.add(IndexedFieldsNames.LINK_ID, x);
//             }
//         }    
//     }


//     private ResponseResourceItem ToResponseResource(Document d)
//     {
//         ResponseResourceItem r = new ResponseResourceItem();
//         r.id = d.get(IndexedFieldsNames.ID);
//         r.appId = d.get(IndexedFieldsNames.APP_ID);
//         r.updateId = d.get(IndexedFieldsNames.UPDATE_ID);
//         r.accountId = d.get(IndexedFieldsNames.ACCOUNT_ID);
//         r.linkId = d.get(IndexedFieldsNames.LINK_ID);
//         r.type = d.get(IndexedFieldsNames.TYPE);
//         r.externalId = d.get(IndexedFieldsNames.EXTERNAL_ID);
//         r.name = d.get(IndexedFieldsNames.NAME);

//         // r.data, r.traits // TODO
        
//         // TODO -> r.indexTime =  d.getField(IndexedFieldsNames.INDEX_TIME).numericValue().longValue();
        
//         return r;
//     }

//     private ResponseItem ToResponseItem(ScoreDoc hit)
//     {
//         ResponseItem resp = new ResponseItem();
//         Document d;
//         try {
//             d = searcher.doc(hit.doc);
//         } catch (IOException e) {
//             throw new RuntimeException();
//         }

//         resp.resource = ToResponseResource(d);
//         resp.sortValue = hit.score; // TODO support using a field etc. in case we dont use default sorting
//         return resp;
//     }

//     public SearchResponse Search(SearchRequest req) throws Exception {
        
//         FacetsCollector fc = new FacetsCollector();
        
//         Query query = req.term == null ? new MatchAllDocsQuery() : queryBuilder.BuildInnerQuery(req.term);

//         // add pre-filters
//         if (req.preFilter != null)
//         {
//             query = queryBuilder.ApplyPrefilter(query, req.preFilter);
//         }

//         // add post-filters
//         DrillDownQuery drilldownQuery = 
//             new DrillDownQuery(facetsConfig, query);
//         ApplyPostfilters(drilldownQuery, req.postFilter);
            
//         // compute sort
//          // TODO - currently by relevance only

//         // set highlights
//          // TODO

//         // run query
//         TopDocs td = FacetsCollector.search(searcher, drilldownQuery, req.paging.skip + req.paging.limit,  fc);   // TODO pagination - only take relevant results. Also - 
//                                                                                                                 // should make sure we correctly adjust this to our "grouping" needs

//         SearchResponse sResp = new SearchResponse();

//         // get results and aggregations
//         ScoreDoc[] hits = td.scoreDocs;
//         if (td.totalHits.value > 0) {
//             sResp.results = Arrays.stream(hits).skip(req.paging.skip).map(x -> ToResponseItem(x)).toArray(ResponseItem[]::new);
//             sResp.count = (int)td.totalHits.value;
//         }
        
//         // now calc facets. TODO: Note that docs says drillSideways should calculate for each drilldown dim, but it seems to do it only for the last added drilldown.
        
//         sResp.filter = GetAggregationSuggestions(query, req.postFilter, remoteTaxoReader, fc)
//             .merge(GetAggregationSuggestions(query, req.postFilter, localTaxoReader, fc));

//         return sResp;
//     }
    
//     private String[] GetFacetResult(Facets facets, String dim) throws IOException
//     {
//         FacetResult r = facets.getTopChildren(50, dim);
//         if (r == null || r.childCount <= 0) 
//             return null;
        
//         return Arrays.stream(r.labelValues).map(x -> x.label).toArray(String[]::new);
//     }

//     private SearchFilter ToAggregationSuggestion(Facets facets) throws IOException
//     {
//         SearchFilter sf = new SearchFilter();
        
//         sf.appId = GetFacetResult(facets, IndexedFieldsNames.APP_ID);
//         sf.resourceType = GetFacetResult(facets, IndexedFieldsNames.TYPE);
//         sf.link = GetFacetResult(facets, IndexedFieldsNames.LINK_ID);

//         return sf;
//     }



//     private SearchFilter GetAggregationSuggestions(Query internalQuery, SearchFilter postFilters, DirectoryTaxonomyReader taxoReader, Collector collector) throws IOException, CloneNotSupportedException
//     {
//         SearchFilter sf = new SearchFilter();
//         DrillSideways ds = new DrillSideways(searcher, facetsConfig, taxoReader);
//         DrillDownQuery dd = new DrillDownQuery(facetsConfig, internalQuery);
//         ApplyPostfilters(dd, postFilters);
//         DrillSidewaysResult dsr = ds.search(dd, collector);
//         sf = ToAggregationSuggestion(dsr.facets);

//         if (postFilters == null || !postFilters.hasAny()) {
//             return sf;
//         }

//         if (postFilters.appId != null && postFilters.appId.length > 0) {
//             dd = new DrillDownQuery(facetsConfig, internalQuery);
//             SearchFilter cpy = postFilters.clone();
//             cpy.appId = null;
//             ApplyPostfilters(dd, cpy);
//             dsr = ds.search(dd, collector);
//             cpy = ToAggregationSuggestion(dsr.facets);
//             sf.appId = cpy.appId;
//         }

//         if (postFilters.resourceType != null && postFilters.resourceType.length > 0) {
//             dd = new DrillDownQuery(facetsConfig, internalQuery);
//             SearchFilter cpy = postFilters.clone();
//             cpy.resourceType = null;
//             ApplyPostfilters(dd, cpy);
//             dsr = ds.search(dd, collector);
//             cpy = ToAggregationSuggestion(dsr.facets);
//             sf.resourceType = cpy.resourceType;
//         }

//         if (postFilters.link != null && postFilters.link.length > 0) {
//             dd = new DrillDownQuery(facetsConfig, internalQuery);
//             SearchFilter cpy = postFilters.clone();
//             cpy.link = null;
//             ApplyPostfilters(dd, cpy);
//             dsr = ds.search(dd, collector);
//             cpy = ToAggregationSuggestion(dsr.facets);
//             sf.link = cpy.link;
//         }

//         return sf;
//     }
    

//     @Override
//     public void close() throws IOException {
//         // TODO Auto-generated method stub
//         if (localTaxoReader != null)
//             localTaxoReader.close();
        
//         if (remoteTaxoReader != null)
//             remoteTaxoReader.close();
//     }
// }
