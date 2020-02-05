/*
 *
 *  *
 *  *  Copyright 2019 Yelp Inc.
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *  *  either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *
 *
 *
 */

package org.apache.platypus.server.luceneserver;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.UUID;
import java.util.stream.Collectors;

public interface Restorable {
    String TMP_SUFFIX = ".tmp";
    Logger logger = LoggerFactory.getLogger(Restorable.class);

    default void restoreDir(Path source, Path target) throws IOException {
        Path baseSource = source.getParent();
        Path tempCurrentLink = baseSource.resolve(getTmpName());
        Path downloadedFileName = Files.list(source).collect(Collectors.toList()).get(0).getFileName();
        Path downloadedData = source.resolve(downloadedFileName);
        logger.info("Point new symlink %s to new data %s".format(target.toString(), downloadedData.toString()));
        Files.createSymbolicLink(tempCurrentLink, downloadedData);
        Files.move(tempCurrentLink, target, StandardCopyOption.REPLACE_EXISTING);
    }

    default String getTmpName() {
        return UUID.randomUUID().toString() + TMP_SUFFIX;
    }

}
