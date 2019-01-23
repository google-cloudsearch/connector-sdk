package com.google.enterprise.cloudsearch.sdk;

import static com.google.enterprise.cloudsearch.sdk.GoogleProxy.TRANSPORT_PROXY_HOSTNAME_KEY;
import static com.google.enterprise.cloudsearch.sdk.GoogleProxy.TRANSPORT_PROXY_PASSWORD_KEY;
import static com.google.enterprise.cloudsearch.sdk.GoogleProxy.TRANSPORT_PROXY_PORT_KEY;
import static com.google.enterprise.cloudsearch.sdk.GoogleProxy.TRANSPORT_PROXY_USERNAME_KEY;
import static org.junit.Assert.assertEquals;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GoogleProxyTest {
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Test
  public void createProxy_emptyConfig_returnsNoProxy() throws Exception {
    setupConfig.initConfig(new Properties());

    assertEquals(Proxy.NO_PROXY, GoogleProxy.fromConfiguration().getProxy());
  }

  @Test
  public void createProxy_proxyCreated() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(TRANSPORT_PROXY_HOSTNAME_KEY, "1.2.3.4");
    properties.setProperty(TRANSPORT_PROXY_PORT_KEY, "1234");
    properties.setProperty(TRANSPORT_PROXY_USERNAME_KEY, "user");
    properties.setProperty(TRANSPORT_PROXY_PASSWORD_KEY, "1234");
    setupConfig.initConfig(properties);

    GoogleProxy proxy = GoogleProxy.fromConfiguration();

    assertEquals(Proxy.Type.HTTP, proxy.getProxy().type());
    assertEquals(new InetSocketAddress("1.2.3.4", 1234), proxy.getProxy().address());

    HttpTransport transport = new MockHttpTransport();
    HttpRequest request = transport.createRequestFactory().buildGetRequest(new GenericUrl());
    proxy.getHttpRequestInitializer().initialize(request);
    assertEquals("Basic dXNlcjoxMjM0", request.getHeaders().get("Proxy-Authorization"));
  }
}
