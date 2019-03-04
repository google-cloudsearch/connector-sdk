package com.google.enterprise.cloudsearch.sdk.sdk;

import com.google.enterprise.cloudsearch.sdk.StatsManager;

/**
 * Convenience class to access the connector's stats.
 */
public class ConnectorStats {

  /**
   * Returns the number of full traversals successfully completed.
   */
  public static int getSuccessfulFullTraversalsCount() {
    return StatsManager
        .getComponent("FullTraverser")
        .getSuccessCount("complete");
  }

  /**
   * Returns the number of incremental traversals successfully completed.
   */
  public static int getSuccessfulIncrementalTraversalsCount() {
    return StatsManager
        .getComponent("IncrementalTraverser")
        .getSuccessCount("complete");
  }
  //TODO(lchandramouli): Get failure count for Full Traversal and Incremental traversal
}
