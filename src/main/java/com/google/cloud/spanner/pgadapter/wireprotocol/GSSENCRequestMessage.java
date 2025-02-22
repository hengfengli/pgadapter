// Copyright 2024 Google LLC
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

package com.google.cloud.spanner.pgadapter.wireprotocol;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.pgadapter.ConnectionHandler;
import com.google.cloud.spanner.pgadapter.wireoutput.DeclineSSLResponse;
import java.io.IOException;
import java.text.MessageFormat;

/** Handles GSSENCRequest bootstrap message. */
@InternalApi
public class GSSENCRequestMessage extends BootstrapMessage {
  private static final int MESSAGE_LENGTH = 8;
  public static final int IDENTIFIER = 80877104; // First Hextet: 1234, Second Hextet: 5680

  private final ThreadLocal<Boolean> executedOnce = ThreadLocal.withInitial(() -> false);

  public GSSENCRequestMessage(ConnectionHandler connection) {
    super(connection, MESSAGE_LENGTH);
  }

  @Override
  protected void sendPayload() throws Exception {
    if (executedOnce.get()) {
      this.connection.handleTerminate();
      throw new IOException("GSSAPI not supported by server");
    }
    // We use the same 'Decline' response for both SSL and GSSAPI.
    new DeclineSSLResponse(this.outputStream).send();
    executedOnce.set(true);
  }

  @Override
  public void nextHandler() throws Exception {
    this.connection.setMessageState(BootstrapMessage.create(this.connection));
  }

  @Override
  protected String getMessageName() {
    return "GSSAPI Request";
  }

  @Override
  protected String getPayloadString() {
    return new MessageFormat("GSSENCRequest, Length: {0}")
        .format(
            new Object[] {
              this.length,
            });
  }

  @Override
  public String getIdentifier() {
    return Integer.toString(IDENTIFIER);
  }
}
