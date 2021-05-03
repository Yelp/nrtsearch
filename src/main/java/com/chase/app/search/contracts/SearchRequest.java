package com.chase.app.search.contracts;

public class SearchRequest 
{
    public String term;
    public SearchFilter preFilter;
    public SearchFilter postFilter;
    public SearchPaging paging;
    public SearchSort sort;
}
