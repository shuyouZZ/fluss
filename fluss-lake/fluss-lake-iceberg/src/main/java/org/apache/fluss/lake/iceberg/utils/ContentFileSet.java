/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.lake.iceberg.utils;

import org.apache.iceberg.ContentFile;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A serializable set of Iceberg content files keyed by their paths.
 *
 * <p>Iceberg 1.10 provides specialized data and delete file sets. This adapter keeps the same
 * path-based de-duplication behavior while remaining compatible with Iceberg 1.6.
 *
 * @param <T> the content-file type
 */
public class ContentFileSet<T extends ContentFile<T>> extends AbstractSet<T>
        implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, T> filesByPath = new LinkedHashMap<>();

    /** Creates an empty content-file set. */
    public static <T extends ContentFile<T>> ContentFileSet<T> create() {
        return new ContentFileSet<>();
    }

    /** Creates a content-file set containing the supplied files. */
    public static <T extends ContentFile<T>> ContentFileSet<T> of(
            Collection<? extends T> contentFiles) {
        ContentFileSet<T> contentFileSet = create();
        contentFileSet.addAll(contentFiles);
        return contentFileSet;
    }

    @Override
    public Iterator<T> iterator() {
        return filesByPath.values().iterator();
    }

    @Override
    public int size() {
        return filesByPath.size();
    }

    @Override
    public boolean contains(Object value) {
        return value instanceof ContentFile
                && filesByPath.containsKey(((ContentFile<?>) value).path().toString());
    }

    @Override
    public boolean add(T contentFile) {
        return filesByPath.putIfAbsent(contentFile.path().toString(), contentFile) == null;
    }

    @Override
    public boolean remove(Object value) {
        if (!(value instanceof ContentFile)) {
            return false;
        }
        return filesByPath.remove(((ContentFile<?>) value).path().toString()) != null;
    }
}
