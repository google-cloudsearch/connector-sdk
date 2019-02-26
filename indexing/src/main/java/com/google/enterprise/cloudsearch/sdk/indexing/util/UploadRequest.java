package com.google.enterprise.cloudsearch.sdk.indexing.util;
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

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonPolymorphicTypeMap;
import com.google.api.client.json.JsonPolymorphicTypeMap.TypeDef;
import com.google.api.client.util.Key;
import com.google.api.services.cloudsearch.v1.model.IndexItemOptions;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PollItemsResponse;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.util.List;

/**
 * This class defines the schema of request to upload to cloud search API.
 *
 * <p><ul><pre>
 *   <li><i>sourceId</i> - Specifies the data source Id.
 *   <li><i>requests</i> - List of requests to be upload to cloud search API.
 * </pre></ul>
 *
 * <p>An example of a file containing requests is:
 * <pre>
 *  {
 *    "sourceId" : "source id",
 *    "requests" : [
 *        {
 *            "name": "deleteItemName",
 *            "type": "items.delete"
 *        }
 *    ]
 *  }
 * </pre>
 */
public class UploadRequest extends GenericJson {

  //sourceId for all the requests
  @Key
  public String sourceId;

  //list of requests related to the above sourceId
  @Key
  public List<? extends AbstractRequest> requests;

  /**
   * Abstract class of the request sent to the indexing API.
   *
   * <p><pre><ul>
   *   <li>The valid types of requests are:
   *        <ul>
   *          <li>items.delete: {@link DeleteRequest}
   *          <li>items.deleteQueueItems: {@link DeleteQueueItemsRequest}
   *          <li>items.get: {@link GetRequest}
   *          <li>items.indexItem: {@link IndexItemRequest}
   *          <li>items.indexItemAndContent: {@link IndexItemAndContentRequest}
   *          <li>items.pollItems: {@link PollItemsRequest}
   *          <li>items.pushItem: {@link PushItemRequest}
   *          <li>items.unreserve: {@link UnreserveRequest}
   *          <li>items.list: {@link ListRequest}
   *          <li>datasources.list: {@link DatasourcesListRequest}
   *          <li>schema.update: {@link UpdateSchemaRequest}
   *          <li>schema.get: {@link GetSchemaRequest}
   *          <li>schema.delete: {@link DeleteSchemaRequest}
   *        </ul>
   *   </ul></pre>
   */
  public abstract static class AbstractRequest extends GenericJson {

    @Key
    @JsonPolymorphicTypeMap(
        typeDefinitions = {
            @TypeDef(key = "items.delete", ref = DeleteRequest.class),
            @TypeDef(key = "items.deleteQueueItems", ref = DeleteQueueItemsRequest.class),
            @TypeDef(key = "items.get", ref = GetRequest.class),
            @TypeDef(key = "items.indexItem", ref = IndexItemRequest.class),
            @TypeDef(key = "items.indexItemAndContent", ref = IndexItemAndContentRequest.class),
            @TypeDef(key = "items.pollItems", ref = PollItemsRequest.class),
            @TypeDef(key = "items.pushItem", ref = PushItemRequest.class),
            @TypeDef(key = "items.unreserve", ref = UnreserveRequest.class),
            @TypeDef(key = "items.list", ref = ListRequest.class),
            @TypeDef(key = "datasources.list", ref = DatasourcesListRequest.class),
            @TypeDef(key = "schema.update", ref = UpdateSchemaRequest.class),
            @TypeDef(key = "schema.get", ref = GetSchemaRequest.class),
            @TypeDef(key = "schema.delete", ref = DeleteSchemaRequest.class),
        }
    )

    public String type;

    /**
     * Get the name of {@link Item} associated with the request.
     *
     * @return the name of the {@link Item}
     */
    abstract String getName();

    abstract GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException;
  }

  /**
   * Abstract class of the request which has its name as the json key.
   *
   * <p>This type of request only needs the name of the item to interact with the indexing API.
   */
  public abstract static class AbstractNameRequest extends AbstractRequest {

    @Key
    String name;

    @Override
    String getName() {
      return name;
    }
  }

  /**
   * Request to delete an Item from the indexing API.
   *
   * <p>An example of DeleteRequest is:
   * <pre>
   *  {
   *    "name": "deleteItemName",
   *    "type": "items.delete"
   *  }
   * </pre>
   */
  public static class DeleteRequest extends AbstractNameRequest {

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to delete items from a given queue.
   *
   * <p>An example of DeleteQueueItemsRequest is:
   * <pre>
   *  {
   *    "queue": "queueName",
   *    "type": "items.deleteQueueItems"
   *  }
   * </pre>
   */
  public static class DeleteQueueItemsRequest extends AbstractRequest {

    @Key
    String queue;

    @Override
    String getName() {
      return queue;
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to get an Item from the indexing API.
   *
   * <p>An example of GetRequest is:
   * <pre>
   *  {
   *    "name": "getItemName",
   *    "type": "items.get"
   *  }
   * </pre>
   */
  public static class GetRequest extends AbstractNameRequest {

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to index an Item to the indexing API.
   *
   * <p><pre><ul>
   * <li><i>item</i> is the {@link Item} object that needs to be updated in the indexing API.
   * <li><i>isIncremental</i> is true if this is to be an incremental update instead of the
   * default backfill.
   * </ul></pre>
   *
   * <p>An example of IndexItemRequest is:
   * <pre>
   *  {
   *    "item": {
   *         "name": "test123",
   *         "item_type": "CONTAINER_ITEM",
   *         "content": {
   *             "content_format": "TEXT"
   *          },
   *          "acl":  {
   *              "readers":
   *              [
   *                  {
   *                      "gsuitePrincipal":
   *                      {
   *                          "gsuiteDomain":true
   *                      }
   *                  }
   *              ]
   *          }
   *    },
   *    "indexItemOptions": {
   *        "allowUnknownGsuitePrincipals":true
   *    },
   *    "type": "items.indexItem",
   *    "isIncremental": "true"
   *  }
   * </pre>
   */
  public static class IndexItemRequest extends AbstractRequest {

    @Key
    Boolean isIncremental;

    @Key
    Item item;

    @Key
    IndexItemOptions indexItemOptions;

    @Override
    String getName() {
      return item.getName();
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to index an Item and its content to the indexing API.
   *
   * <p><pre><ul>
   * <li><i>mediaContent</i> is the content object that needs to be uploaded.
   * </ul></pre>
   *
   * <p>An example of IndexItemAndContentRequest is:
   * <pre>
   *  {
   *    "mediaContent": {
   *        "contentType": "test/json",
   *        "contentString": "testContentString",
   *    },
   *    "item": {
   *         "name": "test567",
   *         "item_type": "CONTAINER_ITEM",
   *         "content": {
   *             "content_format": "TEXT"
   *          },
   *          "acl":  {
   *              "readers":
   *              [
   *                  {
   *                      "gsuitePrincipal":
   *                      {
   *                          "gsuiteDomain":true
   *                      }
   *                  }
   *              ]
   *          }
   *    },
   *    "indexItemOptions": {
   *        "allowUnknownGsuitePrincipals":true
   *    },
   *    "isIncremental": "true",
   *    "type": "items.indexItemAndContent"
   *  }
   * </pre>
   */
  public static class IndexItemAndContentRequest extends IndexItemRequest {

    @Key
    MediaContent mediaContent;

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to poll items from the indexing API.
   *
   * <p><pre><ul>
   * <li><i>limit</i> specifies the maximum number of items to return.
   * <li><i>statusCodes</i> is a list of statuses of {@link Item} that will be polled from the
   * indexing API. Valid status codes are {ERROR, MODIFIED, NEW_ITEM, ACCEPTED}.
   * <li><i>queue</i> is the queue name to fetch items from. If unspecified, it will fetch from
   * "default" queue.
   * </ul>
   * </pre>
   *
   * <p>An example of PollItemsRequest is:
   * <pre>
   *  {
   *    "statusCodes": [
   *        "MODIFIED",
   *        "ACCEPTED"
   *    ],
   *    "limit": 10,
   *    "queue": "glossary",
   *    "type": "items.pollItems"
   *  }
   *  </pre>
   */
  public static class PollItemsRequest extends AbstractRequest {

    @Key
    Integer limit;
    @Key
    List<String> statusCodes;
    @Key
    String queue;

    @Override
    String getName() {
      return String.valueOf(statusCodes);
    }

    @Override
    PollItemsResponse accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to push item to the indexing API.
   *
   * <p><pre><ul>
   * <li><i>pushItem</i> is the {@link PushItem} object to be pushed to the indexing API.
   * </ul></pre>
   *
   * <p>An example of PushItemRequest is:
   * <pre>
   *  {
   *    "pushItem": {
   *        "queue": "testq"
   *    },
   *    "name": "pushItemRequest",
   *    "type": "items.pushItem"
   *  }
   *  </pre>
   */
  public static class PushItemRequest extends AbstractRequest {

    @Key
    PushItem pushItem;

    @Key
    String name;

    @Override
    String getName() {
      return name;
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to unreserve polled {@link Item} instances in a specific queue.
   *
   * <p><pre><ul>
   * <li><i>queue</i> is the name of queue that is unreserved from.
   * </ul></pre>
   *
   * <p>An example of UnreserveRequest is:
   * <pre>
   *   {
   *     "queue": "test",
   *     "type": "items.unreserve"
   *   }
   * </pre>
   */
  public static class UnreserveRequest extends AbstractRequest {

    @Key
    String queue;

    // This method is not used for cloudSearch indexing call
    @Override
    String getName() {
      return MoreObjects.firstNonNull(queue, "default queue");
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to list all items or search items by its attributes specified by filter criteria.
   *
   * <p><pre><ul>
   * <li><i>pageToken</i> is the next_page_token value returned from a previous List request, if
   * any.
   * <li><i>pageSize</i> is the maximum number of {@link Item} to fetch in a request.
   * <li><i>brief</i> is used to shorten {@link
   * com.google.api.services.cloudsearch.v1.model.ItemMetadata} when set to true.
   * </ul></pre>
   *
   * <p>An example of ListRequest is:
   * <pre>
   *   {
   *     "pageSize": 10,
   *     "brief": true,
   *     "type": "items.list"
   *   }
   * </pre>
   */
  public static class ListRequest extends AbstractRequest {

    @Key
    public String pageToken;
    @Key
    public Integer pageSize;
    @Key
    public Boolean brief;

    // This method is not used for cloudSearch indexing call
    @Override
    String getName() {
      return MoreObjects.firstNonNull(pageToken, "default pageToken");
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to list data sources in the indexing API.
   *
   * <p>An example of DatasourcesListRequest is:
   * <pre>
   *   {
   *     "type": "datasources.list"
   *   }
   * </pre>
   */
  public static class DatasourcesListRequest extends AbstractRequest {

    @Key
    public String pageToken;

    @Key
    public Integer pageSize;

    // This method is not used for cloudSearch indexing call
    @Override
    String getName() {
      return "default";
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * MediaContent supports media content from either a url or a string.
   *
   * <p><pre><ul>
   * <li><i>contentType</i> specifies the type of the content.
   * <li><i>url</i> is the url of the content.
   * <li><i>contentString</i> is the content that will be uploaded.
   * </ul></pre>
   *
   * <p>An example of MediaContent is:
   * <pre>
   *   {
   *     "contentType": "test/json",
   *     "url": "content.txt",
   *   }
   * </pre>
   */
  public static class MediaContent extends GenericJson {

    @Key
    public String contentType;
    @Key
    public String url;
    @Key
    public String contentString;
    @Key
    public String contentFormat;
  }

  /**
   * Request to register schema for the data source in the indexing API.
   *
   * <p><pre><ul>
   * <li><i>schemaJsonFile</i> specifies the path to schema json file that needs to be
   * registered.
   * <li><i>validateOnly</i> set to true if the request will be validated without side effects.
   * </ul></pre>
   *
   * <p>An example of UpdateSchemaReqest is:
   * <pre>
   *   {
   *     "schemaJsonFile": "glossarySchema.json",
   *     "type": "schema.update",
   *     "validateOnly": "true"
   *   }
   * </pre>
   */
  public static class UpdateSchemaRequest extends AbstractRequest {

    @Key
    String schemaJsonFile;
    @Key
    Boolean validateOnly;

    // This method is not used for cloudSearch indexing call
    @Override
    String getName() {
      return "default";
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to get the schema from the indexing API for the data source.
   *
   * <p>An example of GetSchemaRequest is:
   * <pre>
   *   {
   *     "type": "schema.get"
   *   }
   * </pre>
   */
  public static class GetSchemaRequest extends AbstractRequest {

    // This method is not used for cloudSearch indexing call
    @Override
    String getName() {
      return "default";
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }

  /**
   * Request to delete the schema from the indexing API for the data source.
   *
   * <p>An example of DeleteSchemaRequest is:
   * <pre>
   *   {
   *     "type": "schema.delete"
   *   }
   * </pre>
   */
  public static class DeleteSchemaRequest extends AbstractRequest {

    // This method is not used for cloudSearch indexing call
    @Override
    String getName() {
      return "default";
    }

    @Override
    GenericJson accept(Uploader.Visitor visitor) throws IOException, InterruptedException {
      return visitor.upload(this);
    }
  }
}
