/*
 * Copyright © 2017 Google Inc.
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
package com.google.enterprise.cloudsearch.csvconnector;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.CloseableIterable;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;
import com.google.enterprise.cloudsearch.sdk.indexing.template.Repository;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.commons.csv.CSVRecord;

/**
 * {@link Repository} implementation for csv connector. {@link CSVConnector} must be used with
 * {@link FullTraversalConnector} template for traversal.
 */
public class CSVRepository implements Repository {
  private CSVFileManager csvFileManager;
  // This can be UNSPECIFIED here and just rely on IndexingService to fill in default / configured
  // value
  private RequestMode requestMode = RequestMode.SYNCHRONOUS;

  public CSVRepository() {
  }

  @Override
  public void init(RepositoryContext repositoryContext) {
    if (!repositoryContext.getDefaultAclMode().isEnabled()) {
      throw new InvalidConfigurationException(
          "defaultAcl.mode must be set, and to a value other than \"none\"");
    }
    csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Override
  public CheckpointCloseableIterable<ApiOperation> getIds(byte[] checkpoint)
      throws RepositoryException {
    return null;
  }

  @Override
  public CheckpointCloseableIterable<ApiOperation> getChanges(byte[] checkpoint)
      throws RepositoryException {
    return new CheckpointCloseableIterableImpl.Builder<ApiOperation>(Collections.emptyIterator())
        .build();
  }

  @Override
  public ApiOperation getDoc(Item item) throws RepositoryException {
    throw new UnsupportedOperationException("Method not yet supported.");
  }

  @Override
  public boolean exists(Item item) throws RepositoryException {
    throw new UnsupportedOperationException("Method not yet supported.");
  }

  @Override
  public void close() {}

  /**
   * Get a result set iterator that supply all the records in CSV files
   *
   * @param checkpoint save state from last iteration
   * @return iterator of database records converted to docs
   * @throws RepositoryException on access errors
   */
  @Override
  public CheckpointCloseableIterable<ApiOperation> getAllDocs(byte[] checkpoint)
      throws RepositoryException {
    return new CheckpointCloseableIterableImpl.Builder<>(new RepositoryDocIterable()).build();
  }

  class RepositoryDocIterable implements CloseableIterable<ApiOperation> {

    private final CloseableIterable<CSVRecord> csvFile;
    private final ResultIterator resultIterator;

    public RepositoryDocIterable() throws RepositoryException {
      try {
        csvFile = csvFileManager.getCSVFile();
      } catch (IOException e) {
        throw new RepositoryException.Builder()
            .setErrorMessage("Error reading the CSV file").setCause(e).build();
      }
      checkNotNull(csvFile);
      resultIterator = new ResultIterator(csvFile.iterator());
    }

    @Override
    public void close() {
      csvFile.close();
    }

    @Override
    public Iterator<ApiOperation> iterator() {
      return resultIterator;
    }

    class ResultIterator implements Iterator<ApiOperation> {
      private Iterator<CSVRecord> csvRecordIterator;

      public ResultIterator(Iterator<CSVRecord> csvRecordIterator) {
        this.csvRecordIterator = csvRecordIterator;
      }

      @Override
      public boolean hasNext() {
        return csvRecordIterator.hasNext();
      }

      @Override
      public ApiOperation next() {
        if (hasNext()) {
          try {
            return createResultSetRecord(csvRecordIterator.next());
          } catch (IOException e) {
            throw new RuntimeException("Error creating record from result set.", e);
          }
        }
        throw new NoSuchElementException();
      }

      RepositoryDoc createResultSetRecord(CSVRecord csvRecord) throws IOException {
        return new RepositoryDoc.Builder()
            .setItem(csvFileManager.createItem(csvRecord))
            .setContent(csvFileManager.createContent(csvRecord), ContentFormat.HTML)
            .setRequestMode(requestMode)
            .build();
      }
    }
  }
}

