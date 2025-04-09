package com.yelp.nrtsearch.server.exceptions;

public class IndexingException extends RuntimeException {


  public IndexingException(String message) {
    super(message);
  }

  public IndexingException(String message, Throwable cause) {
    super(message, cause);
  }

  public IndexingException(Throwable cause) {
    super(cause);
  }

}
