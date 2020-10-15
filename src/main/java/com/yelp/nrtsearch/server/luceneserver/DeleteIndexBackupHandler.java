package com.yelp.nrtsearch.server.luceneserver;

import com.yelp.nrtsearch.server.grpc.DeleteIndexBackupRequest;
import com.yelp.nrtsearch.server.grpc.DeleteIndexBackupResponse;
import com.yelp.nrtsearch.server.utils.Archiver;
import com.yelp.nrtsearch.server.utils.VersionedResourceObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteIndexBackupHandler implements Handler<DeleteIndexBackupRequest, DeleteIndexBackupResponse> {
  Logger logger = LoggerFactory.getLogger(BackupIndexRequestHandler.class);
  private final Archiver archiver;

  public DeleteIndexBackupHandler(Archiver archiver) {
    this.archiver = archiver;
  }

  @Override
  public DeleteIndexBackupResponse handle(IndexState indexState,
      DeleteIndexBackupRequest protoRequest) throws HandlerException {

    DeleteIndexBackupResponse.Builder deleteIndexBackupResponseBuilder =
        DeleteIndexBackupResponse.newBuilder();

    String indexName = protoRequest.getIndexName();

    try {
      String serviceName = protoRequest.getServiceName();
      String resourceName = protoRequest.getResourceName();
      String resourceData = BackupIndexRequestHandler.getResourceData(resourceName);
      String resourceMetadata = BackupIndexRequestHandler.getResourceMetadata(resourceName);
      int nDays = protoRequest.getNDays();

      List<VersionedResourceObject> versionedResourceData =
          archiver.getVersionedResource(serviceName, resourceData);
      List<VersionedResourceObject> versionedResourceMetadata =
          archiver.getVersionedResource(serviceName, resourceMetadata);

      List<VersionedResourceObject> resourceObjects = Stream.concat(
          versionedResourceData.stream(),
          versionedResourceMetadata.stream()
      ).collect(Collectors.toList());

      List<VersionedResourceObject> objectsOlderThanNDays = new ArrayList<>();

      for (VersionedResourceObject obj : resourceObjects) {
        if(olderThanNDays(obj, new Date(), nDays)){
          objectsOlderThanNDays.add(obj);
        }
      }

      for (VersionedResourceObject objectToDelete : objectsOlderThanNDays) {
        archiver.deleteVersion(
            objectToDelete.getServiceName(),
            objectToDelete.getResourceName(),
            objectToDelete.getVersionHash()
        );
      }
    } catch (IOException e) {
      logger.error(
          String.format(
              "Error while trying to delete backup of index %s with serviceName %s, resourceName %s, nDays: %s",
              indexName,
              protoRequest.getServiceName(),
              protoRequest.getResourceName(),
              protoRequest.getNDays()),
          e);
      return deleteIndexBackupResponseBuilder.build();
    }

    return deleteIndexBackupResponseBuilder.build();
  }

  public static boolean olderThanNDays(VersionedResourceObject resourceObject, Date now, int nDays) {
    Calendar c = Calendar.getInstance();
    c.setTime(now);
    c.add(Calendar.DATE, -nDays);
    Date dateNDaysAgo = c.getTime();
    Date dateObjectCreated = resourceObject.getCreationTimestamp();
    return dateObjectCreated.before(dateNDaysAgo);
  }
}
