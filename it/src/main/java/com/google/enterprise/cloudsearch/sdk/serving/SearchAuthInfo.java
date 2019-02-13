package com.google.enterprise.cloudsearch.sdk.serving;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

/**
 * Container for auth info used to serving items.
 *
 * Objects of this class contain the info necessary to authenticate as a given user and perform
 * a serving on their behalf.
 *
 * See https://developers.google.com/adwords/api/docs/guides/authentication#create_a_client_id_and_client_secret
 * for instructions to generate the client secrets.
 */
public class SearchAuthInfo {

  private final File clientSecrets;
  private final File credentialsDirectory;
  private final String userEmail;

  /**
   * Constructor.
   *
   * @param clientSecrets - path to the client secrets JSON file.
   * @param credentialsDirectory - path to the directory containing the StoredCredential file
   *  for the client secrets.
   * @param userEmail - e-mail of the user associated to the client secrets file.
   */
  public SearchAuthInfo(File clientSecrets, File credentialsDirectory, String userEmail) {
    checkArgument(
        clientSecrets.isFile(),
        "client secrets %s doesn't exist",
        clientSecrets.getAbsolutePath());
    checkArgument(
        credentialsDirectory.isDirectory(),
        "credentials directory %s doesn't exist",
        credentialsDirectory.getAbsolutePath());
    this.clientSecrets = clientSecrets;
    this.credentialsDirectory = credentialsDirectory;
    this.userEmail = checkNotNull(userEmail);
  }

  public String getUserEmail() {
    return userEmail;
  }

  public File getCredentialsDirectory() {
    return credentialsDirectory;
  }

  public InputStream getClientSecretsStream() throws FileNotFoundException {
    return new FileInputStream(clientSecrets);
  }
}
