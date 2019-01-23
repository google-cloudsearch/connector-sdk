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
package com.google.enterprise.cloudsearch.sdk.indexing.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Object that simulates a changing repository.
 *
 * <p>This virtual repository contains a fixed number of <em>documents</em> that can change state
 * at each new full (or incremental) traversal of the repository. A document's state is determined
 * by a table that contains a row for each iteration of a full repository traversal, and a column
 * for each <em>document</em>.
 *
 * <p>At each new repository traversal iteration, each document has a state value of either
 * present, missing, or changed. After the table's number of rows is exhausted, the traversal
 * restarts at the beginning of the table.
 */
public class QuickstartSdkDocumentManager {

  private static final String ID_PREFIX = "LIST_SDK_ID";
  private static final int NUMBER_OF_DOCUMENTS = 3;
  private static final int TRAVERSAL_ITERATIONS = 4;

  /**
   * Simple status value of a document.
   */
  private enum DocumentStatus {
    /**
     * The document is present in the repository.
     */
    PRESENT,
    /**
     * The document is not present (missing) in the repository.
     */
    MISSING,
    /**
     * The document is present in the repository, but has modified (changed) content.
     */
    CHANGED
  }

  /**
   * Traversal iteration counter stored as a modulus {@link #TRAVERSAL_ITERATIONS}.
   */
  private int currentIteration = 0;
  private boolean firstTime = true;

  /**
   * Table containing the state of each document at each iteration.
   */
  private final DocumentStatus[][] docTable =
      new DocumentStatus[TRAVERSAL_ITERATIONS][NUMBER_OF_DOCUMENTS];

  public QuickstartSdkDocumentManager() {
    setUpDocumentTable();
  }

  /**
   * Sets up the document status for each iteration.
   *
   * <p>For each iteration of a traversal, this table defines the status of each document in the
   * simulated repository. This is a circular table, so after the last row is processed, the index
   * is set back to the beginning.
   *
   * <p>For this example, the following table defines the desired test scenario. Each row
   * represents an iteration.
   *
   * <table>
   * <tr> <th>Document 1</th> <th>Document 2</th> <th>Document 3</th> </tr>
   * <tr> <td>Present</td> <td>Present</td> <td>Present</td> </tr>
   * <tr> <td>Present</td> <td>Present</td> <td>Missing</td> </tr>
   * <tr> <td>Present</td> <td>Changed</td> <td>Present</td> </tr>
   * <tr> <td>Present</td> <td>Present*</td> <td>Present</td> </tr>
   * </table>
   *
   * <p>* - This is detected "changed" when compared with the previous content.
   */
  private void setUpDocumentTable() {
    // iteration 0
    docTable[0][0] = DocumentStatus.PRESENT;
    docTable[0][1] = DocumentStatus.PRESENT;
    docTable[0][2] = DocumentStatus.PRESENT;
    // iteration 1
    docTable[1][0] = DocumentStatus.PRESENT;
    docTable[1][1] = DocumentStatus.PRESENT;
    docTable[1][2] = DocumentStatus.MISSING;
    // iteration 2
    docTable[2][0] = DocumentStatus.PRESENT;
    docTable[2][1] = DocumentStatus.CHANGED;
    docTable[2][2] = DocumentStatus.PRESENT;
    // iteration 3
    docTable[3][0] = DocumentStatus.PRESENT;
    docTable[3][1] = DocumentStatus.PRESENT;
    docTable[3][2] = DocumentStatus.PRESENT;
  }

  /**
   * Increments the index of the traversal count.
   *
   * <p>The quickstart {@link com.google.enterprise.cloudsearch.sdk.indexing.template.Repository}
   * typically
   * calls this method at the start of each scheduled traversal to update the iteration count.
   */
  public synchronized void initializeForNextTraversal() {
    if (firstTime) {
      firstTime = false;
      currentIteration = 0;
    } else {
      currentIteration = ++currentIteration % TRAVERSAL_ITERATIONS;
    }
  }

  // all document ID numbers are 1-based integers
  private boolean isPresent(int docIdNumber) {
    return docTable[currentIteration][docIdNumber - 1] == DocumentStatus.PRESENT;
  }

  private boolean isMissing(int docIdNumber) {
    return docTable[currentIteration][docIdNumber - 1] == DocumentStatus.MISSING;
  }

  private boolean isChanged(int docIdNumber) {
    return docTable[currentIteration][docIdNumber - 1] == DocumentStatus.CHANGED;
  }

  /**
   * Returns whether the document has had a change of status since the last traversal/iteration.
   *
   * @param docIdNumber the document number (1-based)
   * @return {@code true} only if the document's status has changed from the previous iteration
   */
  private boolean isDifferentNow(int docIdNumber) {
    int lastIteration = currentIteration == 0 ? TRAVERSAL_ITERATIONS - 1 : currentIteration - 1;
    DocumentStatus lastStatus = docTable[lastIteration][docIdNumber - 1];

    DocumentStatus currentStatus = docTable[currentIteration][docIdNumber - 1];

    return currentStatus != lastStatus;
  }

  /**
   * Creates a unique document ID (name) from the document number.
   *
   * @param documentNumber the document number (1-based)
   * @return fully formed unique document ID
   */
  private String getUniqueId(int documentNumber) {
    return ID_PREFIX + documentNumber;
  }

  /**
   * Extracts the document number for the ID string.
   *
   * @param id the document ID
   * @return the document number
   */
  private int getDocumentNumberFromId(String id) {
    return Integer.parseInt(id.substring(ID_PREFIX.length()));
  }

  /**
   * Determines whether the document is present in the data repository.
   *
   * <p>If the ID does not represent a document in this data repository, it is considered
   * non-existent. This may have the side effect of indicating to the calling connector code to
   * delete this document. It is therefore recommended that no other connectors use this same data
   * source for their documents.
   */
  public boolean documentExists(String id) {
    if ((id.length() < ID_PREFIX.length())
        || !id.substring(0, ID_PREFIX.length()).equals(ID_PREFIX)) {
      return false;
    }

    return !isMissing(getDocumentNumberFromId(id));
  }

  /**
   * Returns a standard or modified content string based on the document's current status.
   *
   * @param id the document id
   * @return a content string based on document status
   */
  public String getContent(String id) {
    int documentNumber = getDocumentNumberFromId(id);
    if (isChanged(documentNumber)) {
      return "Hello world <modified> (" + id + ")";
    } else {
      return "Hello world (" + id + ")";
    }
  }

  /**
   * Returns the document's content hash value.
   *
   * @param id the document's ID
   * @return the document's content hash value
   */
  public String getContentHash(String id) {
    return Integer.toString(Objects.hash(getContent(id)));
  }

  /**
   * Returns only the existing (present and changed) document IDs in the repository during the
   * current traversal iteration.
   *
   * @return list of document IDs (1-based)
   */
  public List<String> getAllDocumentIds() {
    List<String> allDocs = new ArrayList<>();
    for (int docNumber = 1; docNumber <= NUMBER_OF_DOCUMENTS; docNumber++) {
      if (!isMissing(docNumber)) {
        allDocs.add(getUniqueId(docNumber));
      }
    }
    return allDocs;
  }

  /**
   * Returns only modified document IDs in the repository during the current traversal iteration.
   *
   * <p>The <em>modified</em> documents are defined as having a new status when compared to the
   * previous traversal (iteration). This means that a previously <em>present</em> document that is
   * now <em>missing</em> is considered a <em>modified</em> document. A previously <em>changed</em>
   * document that is still <em>changed</em> is not considered <em>modified</em>.
   *
   * @return list of document IDs (1-based)
   */
  public List<String> getModifiedDocumentIds() {
    List<String> modifiedDocs = new ArrayList<>();
    for (int docNumber = 1; docNumber <= NUMBER_OF_DOCUMENTS; docNumber++) {
      if (isDifferentNow(docNumber)) {
        modifiedDocs.add(getUniqueId(docNumber));
      }
    }
    return modifiedDocs;
  }
}
