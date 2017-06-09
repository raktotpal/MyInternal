package com.internal.GoogleDriveAPI;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.FileList;

/**
 * Turn on the Drive API
 * 
 * <ol>
 * <li>
 * Use this wizard to create or select a project in the Google Developers
 * Console and automatically turn on the API.
 * <li>
 * Click Continue, then Go to credentials.
 * <li>
 * On the Add credentials to your project page, click the Cancel button.
 * <li>
 * At the top of the page, select the OAuth consent screen tab.
 * <li>
 * Select an Email address, enter a Product name if not already set, and click
 * the Save button.
 * <li>
 * Select the Credentials tab, click the Create credentials button and select
 * OAuth client ID.
 * <li>
 * Select the application type Other, enter the name "Drive API Quickstart", and
 * click the Create button.
 * <li>
 * Click OK to dismiss the resulting dialog.
 * <li>
 * Click the file_download (Download JSON) button to the right of the client ID.
 * <li>
 * Move this file to your working directory and rename it client_secret.json.
 * 
 * @author RAKTOTPAL
 *
 */
public class TestExecutor {
  /** Application name. */
  private static final String APPLICATION_NAME = "Drive API Java Quickstart";

  /** Directory to store user credentials for this application. */
  private static final java.io.File DATA_STORE_DIR = new java.io.File(
      System.getProperty("user.home"), ".credentials/drive-java-quickstart");

  /** Global instance of the {@link FileDataStoreFactory}. */
  private static FileDataStoreFactory DATA_STORE_FACTORY;

  /** Global instance of the JSON factory. */
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /** Global instance of the HTTP transport. */
  private static HttpTransport HTTP_TRANSPORT;

  /** Global instance of the scopes required by this quickstart. */
  private static final List<String> SCOPES = Arrays.asList(DriveScopes.DRIVE_METADATA_READONLY);

  static {
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
    } catch (Throwable t) {
      t.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Creates an authorized Credential object.
   * 
   * @return an authorized Credential object.
   * @throws Exception
   */
  public static Credential authorize() throws Exception {
    // Load client secrets.
    InputStream in = TestExecutor.class.getResourceAsStream("/client_secret.json");
    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
        new InputStreamReader(in));

    // Build flow and trigger user authorization request.
    GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(HTTP_TRANSPORT,
        JSON_FACTORY, clientSecrets, SCOPES).setDataStoreFactory(DATA_STORE_FACTORY)
        .setAccessType("offline").build();
    Credential credential = new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver())
        .authorize("user");
    System.out.println("Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());
    return credential;
  }

  /**
   * Build and return an authorized Drive client service.
   * 
   * @return an authorized Drive client service
   * @throws Exception
   */
  public static Drive getDriveService() throws Exception {
    Credential credential = authorize();
    return new Drive.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName(
        APPLICATION_NAME).build();
  }

  public static void main(String[] args) throws Exception {
    // Build a new authorized API client service.
    Drive service = getDriveService();

    // Print the names and IDs for up to 10 files.
    FileList result = service.files().list().setPageSize(10)
        .setFields("nextPageToken, files(id, name)").execute();
    List<File> files = result.getFiles();
    if (files == null || files.size() == 0) {
      System.out.println("No files found.");
    } else {
      System.out.println("Files:");
      for (File file : files) {
        System.out.printf("%s (%s)\n", file.getName(), file.getId());
      }
    }
  }

  /**
   * Insert new file.
   *
   * @param service
   *          Drive API service instance.
   * @param title
   *          Title of the file to insert, including the extension.
   * @param description
   *          Description of the file to insert.
   * @param parentId
   *          Optional parent folder's ID.
   * @param mimeType
   *          MIME type of the file to insert.
   * @param filename
   *          Filename of the file to insert.
   * @return Inserted file metadata if successful, {@code null} otherwise.
   */
  private static File insertFile(Drive service, String description, String parentId,
      String mimeType, String filename) {
    // File's metadata.
    File body = new File();
    // body.setTitle(title);
    body.setDescription(description);
    body.setMimeType(mimeType);

    // Set the parent folder.
    if (parentId != null && parentId.length() > 0) {
      // body.setParents(
      // Arrays.asList(new ParentReference().setId(parentId)));
    }

    // File's content.
    java.io.File fileContent = new java.io.File(filename);
    FileContent mediaContent = new FileContent(mimeType, fileContent);
    // try {
    // File file = service.files().insert(body, mediaContent).execute();

    // Uncomment the following line to print the File ID.
    // System.out.println("File ID: " + file.getId());

    // return file;
    return null;
    // } catch (IOException e) {
    // System.out.println("An error occured: " + e);
    // return null;
    // }
  }

  // ...
}