/*
 * Copyright 2012 Google Inc. All Rights Reserved.
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

package com.google.appengine.tools.cloudstorage.oauth;

import com.google.api.client.util.Joiner;
import com.google.api.client.util.Lists;
import com.google.appengine.api.ThreadManager;
import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.URLFetchServiceFactory;
import com.google.appengine.repackaged.com.google.common.base.Function;
import com.google.appengine.repackaged.com.google.common.util.concurrent.Futures;
import com.google.appengine.tools.cloudstorage.RawGcsService;
import com.google.appengine.tools.cloudstorage.RetryHelperException;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Factory for RawGcsService using OAuth for authorization.
 */
public final class OauthRawGcsServiceFactory {

  private static final AppIdentityURLFetchService appIdFetchService =
      new AppIdentityURLFetchService(getURLFetchService(), OauthRawGcsService.OAUTH_SCOPES);

  private OauthRawGcsServiceFactory() {}

  /**
   * @param headers
   * @return a new RawGcsService
   */
  public static RawGcsService createOauthRawGcsService(ImmutableSet<HTTPHeader> headers) {
    return new OauthRawGcsService(appIdFetchService, headers);
  }

  public static URLFetchService getURLFetchService() {
    return true || Boolean.parseBoolean(System.getenv("GAE_VM"))
        ? new URLConnectionAdapter() : new URLFetchAdapter();
  }

  private static class URLConnectionAdapter implements URLFetchService {

    private static final ExecutorService executor =
        Executors.newSingleThreadExecutor(ThreadManager.currentRequestThreadFactory());

    @Override
    public HTTPResponse fetch(HTTPRequest req) throws IOException, RetryHelperException {
      URL url = req.getURL();
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(req.getMethod().name());
      for (HTTPHeader header : req.getHeaders()) {
        connection.setRequestProperty(header.getName(), header.getValue());
      }
      // TODO: We should apply HTTPRequest options on the connection but the HTTPRequest
      // accessors are package scope. For now set the values based on OauthRawGcsService.makeRequest
      connection.setInstanceFollowRedirects(false);
      connection.setConnectTimeout(20_000);
      connection.setReadTimeout(30_000);
      byte[] payload = req.getPayload();
      if (payload != null) {
        connection.setDoOutput(true);
        OutputStream wr = connection.getOutputStream();
        wr.write(payload);
        wr.flush();
        wr.close();
      }
      final int responseCode = connection.getResponseCode();
      final byte[] content = ByteStreams.toByteArray(connection.getInputStream());
      final Map<String, List<String>> headers = connection.getHeaderFields();
      return new HTTPResponse() {

        @Override
        public int getResponseCode() {
          return responseCode;
        }

        @Override
        public byte[] getContent() {
          return content;
        }

        @Override
        public List<HTTPHeader> getHeadersUncombined() {
          List<HTTPHeader> response = Lists.newArrayListWithCapacity(headers.size());
          for (Map.Entry<String, List<String>> h : headers.entrySet()) {
            for (String v : h.getValue()) {
              response.add(new HTTPHeader(h.getKey(), v));
            }
          }
          return response;
        }

        @Override
        public List<HTTPHeader> getHeaders() {
          List<HTTPHeader> response = Lists.newArrayListWithCapacity(headers.size());
          for (Map.Entry<String, List<String>> h : headers.entrySet()) {
            response.add(new HTTPHeader(h.getKey(), Joiner.on(',').join(h.getValue())));
          }
          return response;
        }
      };
    }

    @Override
    public Future<HTTPResponse> fetchAsync(final HTTPRequest request) {
      return executor.submit(new Callable<HTTPResponse>() {

        @Override public HTTPResponse call() throws Exception {
          return fetch(request);
        }
      });
    }
  }

  private static class URLFetchAdapter implements URLFetchService {

    private static com.google.appengine.api.urlfetch.URLFetchService delegate =
        URLFetchServiceFactory.getURLFetchService();

    @Override
    public HTTPResponse fetch(HTTPRequest req) throws IOException, RetryHelperException {
      final com.google.appengine.api.urlfetch.HTTPResponse r = delegate.fetch(req);
      return new HTTPResponseAdapter(r);
    }

    @Override
    public Future<HTTPResponse> fetchAsync(HTTPRequest req) {
      final Future<com.google.appengine.api.urlfetch.HTTPResponse> r = delegate.fetchAsync(req);
      return Futures.lazyTransform(r,
          new Function<com.google.appengine.api.urlfetch.HTTPResponse, HTTPResponse>() {

            @Override
            public HTTPResponse apply(com.google.appengine.api.urlfetch.HTTPResponse r) {
              return new HTTPResponseAdapter(r);
            }
          });
    }

    private static class HTTPResponseAdapter implements HTTPResponse {

      private final com.google.appengine.api.urlfetch.HTTPResponse r;

      public HTTPResponseAdapter(com.google.appengine.api.urlfetch.HTTPResponse r) {
        this.r = r;
      }

      @Override
      public int getResponseCode() {
        return r.getResponseCode();
      }

      @Override
      public byte[] getContent() {
        return r.getContent();
      }

      @Override
      public List<HTTPHeader> getHeadersUncombined() {
        return r.getHeadersUncombined();
      }

      @Override
      public List<HTTPHeader> getHeaders() {
        return r.getHeaders();
      }
    }
  }
}
