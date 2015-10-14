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

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.URLFetchServiceFactory;
import com.google.appengine.repackaged.com.google.common.base.Function;
import com.google.appengine.repackaged.com.google.common.util.concurrent.Futures;
import com.google.appengine.tools.cloudstorage.RawGcsService;
import com.google.appengine.tools.cloudstorage.RetryHelperException;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.List;
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
    return new URLFetchAdapter();
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
