/*
 * Copyright 2015 The gRPC Authors
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

package io.grpc.examples.analytics;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.wso2.analytics.mgw.grpc.service.AnalyticsSendServiceGrpc;
import org.wso2.analytics.mgw.grpc.service.AnalyticsStreamMessage;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sample client code that makes gRPC calls to the server.
 */
public class AnalyticsClient {
  private static final Logger logger = Logger.getLogger(AnalyticsClient.class.getName());

  //private final RouteGuideBlockingStub blockingStub;
  //private final RouteGuideStub asyncStub;
  private AnalyticsSendServiceGrpc.AnalyticsSendServiceStub asyncStub;

  private Random random = new Random();
  private TestHelper testHelper;

  /** Construct client for accessing RouteGuide server using the existing channel. */
  public AnalyticsClient(/*Channel channel*/) {
    //blockingStub = RouteGuideGrpc.newBlockingStub(channel);
    //asyncStub = RouteGuideGrpc.newStub(channel);
    //asyncStub = AnalyticsSendServiceGrpc.newStub(channel);
  }

  public void sendAnalytics() throws InterruptedException {
    info("*** sendAnalytics***");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<Empty> responseObserver = new StreamObserver<Empty>() {
      @Override
      public void onNext(Empty empty) {
        info("Finished trip with sendAnalytics Empty Payload response");
        if (testHelper != null) {
          testHelper.onMessage(empty);
        }
      }

      @Override
      public void onError(Throwable t) {
        warning("sendAnalytics Failed: {0}", Status.fromThrowable(t));
        if (testHelper != null) {
          testHelper.onRpcError(t);
        }
        finishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        info("Finished sendAnalytics");
        finishLatch.countDown();
      }
    };

    StreamObserver<AnalyticsStreamMessage> requestObserver = asyncStub.sendAnalytics(responseObserver);
    try {
      // Send numPoints points randomly selected from the features list.
      for (int i = 0; i < 10; ++i) {
        //int index = random.nextInt(10);
        //Point point = features.get(index).getLocation();
       //*//* info("Visiting point {0}, {1}", RouteGuideUtil.getLatitude(point),
        //RouteGuideUtil.getLongitude(point));*//*
        requestObserver.onNext(AnalyticsStreamMessage.newBuilder().
                setMessageStreamName("InComingRequestStream").setMetaClientType("PRODUCTION").
                setApplicationConsumerKey("OauthClient_for_Illion").setApplicationName("OauthClient_for_Illion").
                setApplicationId("6").setApplicationOwner("admin").setApiContext("/banking/capability/party" +
                "-management/v1").setApiName("party-management-api").setApiVersion("v1").setApiResourcePath("/parties" +
                "/search").setApiResourceTemplate("/parties/search").setApiMethod("POST").setApiCreator("admin").
                setApiCreatorTenantDomain("carbon.super").setApiTier("").setApiHostname("localhost:9095").
                setUsername("admin@carbon.super").setUserTenantDomain("carbon.super").setUserIp("0:0:0:0:0:0:0:1").
                setUserAgent("PostmanRuntime/7.26.10").setRequestTimestamp(1615514498484L).setThrottledOut(false).
                setResponseTime(2257L).setServiceTime(1327L).setBackendTime(930L).setResponseCacheHit(false).
                setResponseSize(4442L).setProtocol("http").setResponseCode(200).
                setDestination("http://www.mocky.io/v2/5ec34681300000890039bf5c").setSecurityLatency(7L).
                setThrottlingLatency(38L).setRequestMedLat(0L).setResponseMedLat(0L).setBackendLatency(930L).
                setOtherLatency(0L).setGatewayType("MICRO").setLabel("MICRO").setSubscriber("").setThrottledOutReason("").
                setThrottledOutTimestamp(0L).setHostname("").setErrorMessage("").setErrorCode("").build());
        // Sleep for a bit before sending the next one.
        Thread.sleep(random.nextInt(1000) + 500);
        if (finishLatch.getCount() == 0) {
          // RPC completed or errored before we finished sending.
          // Sending further requests won't error, but they will just be thrown away.
          return ;
        }
      }
    } catch (RuntimeException | InterruptedException e) {
      // Cancel RPC
      requestObserver.onError(e);
      e.printStackTrace();
    }
    // Mark the end of requests
    requestObserver.onCompleted();

    // Receiving happens asynchronously
    if (!finishLatch.await(1, TimeUnit.MINUTES)) {
      warning("sendAnalytics can not finish within 1 minutes");
    }
  }

  public void executeClient() throws InterruptedException, URISyntaxException {



    Properties prop = new Properties();
    try (InputStream input = new FileInputStream(System.getProperty("user.dir") + File.separator + "config.properties")) {


      // load a properties file
      prop.load(input);

      // get the property value and print it out
      System.out.println("prop.getProperty('aw.host') :" + prop.getProperty("aw.host"));
      System.out.println("prop.getProperty('truststore.password') :" + prop.getProperty("truststore.password"));
      System.out.println("prop.getProperty('truststore.name') :" + prop.getProperty("truststore.name"));

    } catch (IOException ex) {
      ex.printStackTrace();
    }
    System.setProperty("https.protocols", "TLSv1.2");
    //String absolutePath = System.getProperty("user.dir") + File.separator + "client-truststore.jks";
    String absolutePath = System.getProperty("user.dir") + File.separator + prop.getProperty("truststore.name");
    System.setProperty("javax.net.ssl.trustStore", absolutePath);
    System.setProperty("javax.net.ssl.trustStorePassword",prop.getProperty("truststore.password"));


    ManagedChannel channel = ManagedChannelBuilder.forTarget(prop.getProperty("aw.host")).useTransportSecurity().build();

    try {
      asyncStub = AnalyticsSendServiceGrpc.newStub(channel);
      sendAnalytics();
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }
  }


  /** Issues several different requests and then exits. */
  public static void main(String[] args) throws InterruptedException, URISyntaxException {

    AnalyticsClient analyticsClient = new AnalyticsClient();
    analyticsClient.executeClient();
  }

  private void info(String msg, Object... params) {
    logger.log(Level.INFO, msg, params);
  }

  private void warning(String msg, Object... params) {
    logger.log(Level.WARNING, msg, params);
  }


  /**
   * Only used for unit test, as we do not want to introduce randomness in unit test.
   */
  @VisibleForTesting
  void setRandom(Random random) {
    this.random = random;
  }

  /**
   * Only used for helping unit test.
   */
  @VisibleForTesting
  interface TestHelper {
    /**
     * Used for verify/inspect message received from server.
     */
    void onMessage(Message message);

    /**
     * Used for verify/inspect error received from server.
     */
    void onRpcError(Throwable exception);
  }

  @VisibleForTesting
  void setTestHelper(TestHelper testHelper) {
    this.testHelper = testHelper;
  }
}
