package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.utils.VersionedResourceObject;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

public class DeleteIndexBackupHandlerTest {

  @Test
  public void testOlderThanNDays() {

    Date now = new Date(2020, 5, 25);
    Date date1 = new Date(2020, 5, 1);
    Date date2 = new Date(2020, 4, 20);

    int nDays = 30;

    VersionedResourceObject testObj1 = VersionedResourceObject.builder()
        .setCreationTimestamp(date1)
        .createVersionedResourceObject();

    Assert.assertEquals(false, DeleteIndexBackupHandler.olderThanNDays(testObj1, now, nDays));

    VersionedResourceObject testObj2 = VersionedResourceObject.builder()
        .setCreationTimestamp(date2)
        .createVersionedResourceObject();

    Assert.assertEquals(true, DeleteIndexBackupHandler.olderThanNDays(testObj2, now, nDays));
  }
}
