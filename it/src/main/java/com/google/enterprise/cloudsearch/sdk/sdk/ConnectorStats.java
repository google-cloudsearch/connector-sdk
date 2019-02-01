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

}
