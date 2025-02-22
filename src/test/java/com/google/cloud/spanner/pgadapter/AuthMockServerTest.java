// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.cloud.spanner.pgadapter.metadata.OptionsMetadata.Builder;
import com.google.cloud.spanner.pgadapter.wireprotocol.PasswordMessage;
import java.io.IOException;
import java.io.StringWriter;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.util.PSQLException;

@RunWith(JUnit4.class)
public class AuthMockServerTest extends AbstractMockServerTest {
  static final String CREDENTIALS_ERROR =
      "ERROR: Invalid credentials received.\n"
          + "  Hint: PGAdapter expects credentials to be one of the following:\n"
          + "1. Username contains the fixed string 'oauth2' and the password field contains a valid OAuth2 token.\n"
          + "2. Username contains any string and the password field contains the JSON payload of a service account or user account credentials file. Note: Only user accounts and service accounts are supported.\n"
          + "3. Username contains the email address of a service account and the password contains the corresponding private key for the service account.";

  @BeforeClass
  public static void loadPgJdbcDriver() throws Exception {
    // Make sure the PG JDBC driver is loaded.
    Class.forName("org.postgresql.Driver");
  }

  @BeforeClass
  public static void startMockSpannerAndPgAdapterServers() throws Exception {
    // Start PGAdapter with authentication mode enabled.
    doStartMockSpannerAndPgAdapterServers("d", Builder::setRequireAuthentication);
  }

  private String createUrl() {
    return String.format("jdbc:postgresql://localhost:%d/d", pgServer.getLocalPort());
  }

  @Test
  public void testConnectFailsWithoutAuth() {
    // The server is started with authentication required. Trying to obtain a connection without any
    // credentials will fail.
    SQLException exception =
        assertThrows(SQLException.class, () -> DriverManager.getConnection(createUrl()));
    assertTrue(
        exception.getMessage(),
        exception
            .getMessage()
            .contains(
                "The server requested password-based authentication, but no password was provided"));
  }

  @Test
  public void testConnectFailsWithRandomAuth() {
    PSQLException exception =
        assertThrows(
            PSQLException.class, () -> DriverManager.getConnection(createUrl(), "foo", "bar"));
    assertEquals(CREDENTIALS_ERROR, exception.getMessage());
  }

  @Test
  public void testConnectFailsWithEmptyUsername() {
    PSQLException exception =
        assertThrows(
            PSQLException.class,
            () -> DriverManager.getConnection(createUrl(), "", "any-random-oauth2-token"));
    assertEquals(CREDENTIALS_ERROR, exception.getMessage());
  }

  @Test
  public void testConnectFailsWithEmptyPassword() {
    PSQLException exception =
        assertThrows(
            PSQLException.class, () -> DriverManager.getConnection(createUrl(), "oauth2", ""));
    assertEquals(CREDENTIALS_ERROR, exception.getMessage());
  }

  @Test
  public void testConnectWithOAuth2Token() throws Exception {
    String username = "oauth2";
    String password = "any-random-oauth2-token";
    // Note that even though we are sending credentials here, these will never reach the mock
    // server. The Connection API will never send credentials over a plain-text connection to
    // Spanner.
    try (Connection connection = DriverManager.getConnection(createUrl(), username, password)) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }
    }

    PasswordMessage passwordMessage =
        pgServer.getDebugMessages().stream()
            .filter(message -> message instanceof PasswordMessage)
            .map(message -> (PasswordMessage) message)
            .findAny()
            .orElseGet(() -> mock(PasswordMessage.class));
    assertNotNull(passwordMessage);
    assertEquals(username, passwordMessage.getUsername());
    assertEquals(password, passwordMessage.getPassword());
  }

  @Test
  public void testConnectWithPrivateKey() throws Exception {
    String username = "foo@bar.com";
    String password = generateRandomPrivateKey();
    // Note that even though we are sending credentials here, these will never reach the mock
    // server. The Connection API will never send credentials over a plain-text connection to
    // Spanner.
    try (Connection connection = DriverManager.getConnection(createUrl(), username, password)) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }
    }

    PasswordMessage passwordMessage =
        pgServer.getDebugMessages().stream()
            .filter(message -> message instanceof PasswordMessage)
            .map(message -> (PasswordMessage) message)
            .findAny()
            .orElseGet(() -> mock(PasswordMessage.class));
    assertNotNull(passwordMessage);
    assertEquals("foo@bar.com", passwordMessage.getUsername());
    assertEquals(password, passwordMessage.getPassword());
  }

  /**
   * String with a randomly generated credentials file (NOTE: These are not valid and cannot be used
   * anywhere).
   */
  private static final String RANDOM_CREDENTIALS_FILE =
      "{\n"
          + "  \"type\": \"service_account\",\n"
          + "  \"project_id\": \"pgadapter-mock-server-test-project\",\n"
          + "  \"private_key_id\": \"random-key-id\",\n"
          + "  \"private_key\": \"%s\",\n"
          + "  \"client_email\": \"pgadapter-test-account@pgadapter-mock-server-test-project.iam.invalid.com\",\n"
          + "  \"client_id\": \"123456789\",\n"
          + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
          + "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n"
          + "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n"
          + "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/invalid.com\"\n"
          + "}\n";

  private static final String INVALID_CREDENTIALS_FILE =
      "{\n"
          + "  \"type\": \"external_account\",\n"
          + "  \"project_id\": \"pgadapter-mock-server-test-project\",\n"
          + "  \"private_key_id\": \"random-key-id\",\n"
          + "  \"private_key\": \"%s\",\n"
          + "  \"client_email\": \"pgadapter-test-account@pgadapter-mock-server-test-project.iam.invalid.com\",\n"
          + "  \"client_id\": \"123456789\",\n"
          + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
          + "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n"
          + "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n"
          + "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/invalid.com\"\n"
          + "}\n";

  @Test
  public void testConnectWithCredentialsFile() throws Exception {
    // The username is ignored by PGAdapter when a credentials file is used.
    String username = "whatever";
    String password = String.format(RANDOM_CREDENTIALS_FILE, generateRandomPrivateKey());
    // Note that even though we are sending credentials here, these will never reach the mock
    // server. The Connection API will never send credentials over a plain-text connection to
    // Spanner.
    try (Connection connection = DriverManager.getConnection(createUrl(), username, password)) {
      try (ResultSet resultSet = connection.createStatement().executeQuery(SELECT1.getSql())) {
        assertTrue(resultSet.next());
        assertEquals(1, resultSet.getInt(1));
        assertFalse(resultSet.next());
      }
    }

    PasswordMessage passwordMessage =
        pgServer.getDebugMessages().stream()
            .filter(message -> message instanceof PasswordMessage)
            .map(message -> (PasswordMessage) message)
            .findAny()
            .orElseGet(() -> mock(PasswordMessage.class));
    assertNotNull(passwordMessage);
    assertEquals("whatever", passwordMessage.getUsername());
    assertEquals(password, passwordMessage.getPassword());
  }

  @Test
  public void testConnectWithInvalidCredentialsFile() throws Exception {
    // The username is ignored by PGAdapter when a credentials file is used.
    String username = "whatever";
    String password = String.format(INVALID_CREDENTIALS_FILE, generateRandomPrivateKey());
    PSQLException exception =
        assertThrows(
            PSQLException.class,
            () -> DriverManager.getConnection(createUrl(), username, password));
    assertEquals(CREDENTIALS_ERROR, exception.getMessage());
  }

  private String generateRandomPrivateKey() throws NoSuchAlgorithmException, IOException {
    final KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048, null);
    final KeyPair keyPair = generator.generateKeyPair();
    final PrivateKey privateKey = keyPair.getPrivate();
    final StringWriter stringWriter = new StringWriter();
    try (JcaPEMWriter pemWriter = new JcaPEMWriter(stringWriter)) {
      pemWriter.writeObject(new JcaPKCS8Generator(privateKey, null));
    }
    return stringWriter.toString();
  }
}
