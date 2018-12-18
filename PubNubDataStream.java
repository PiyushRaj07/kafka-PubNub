package com.Pubnub.PubNub;
import java.util.Arrays;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.PNCallback;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNPublishResult;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;
//import com.sengence.kafka.connect.pubnub.TwitterKafkaProducer;




public class PubNubDataStream {

  public static void main( String[] args )
	{
		  
	PNConfiguration pnConfiguration = new PNConfiguration();
    pnConfiguration.setSubscribeKey("Keys-for-subscription");
    pnConfiguration.setPublishKey("demo");
     
    PubNub pubnub = new PubNub(pnConfiguration);
 
    final String channelName = "pubnub-sensor-network";
 

    // create message payload using Gson
    final JsonObject messageJsonObject = new JsonObject();
    messageJsonObject.addProperty("msg", "hello");
 
    System.out.println("Message to send: " + messageJsonObject.toString());
	 
	    pubnub.addListener(new SubscribeCallback() {
	        @Override
	        public void status(PubNub pubnub, PNStatus status) {
	 
	 
	            if (status.getCategory() == PNStatusCategory.PNUnexpectedDisconnectCategory) {
	                // This event happens when radio / connectivity is lost
	            }
	 
	            else if (status.getCategory() == PNStatusCategory.PNConnectedCategory) {
	 
	                // Connect event. You can do stuff like publish, and know you'll get it.
	            // Or just use the connected event to confirm you are subscribed for
	            // UI / internal notifications, etc
	         
	            if (status.getCategory() == PNStatusCategory.PNConnectedCategory){
	                pubnub.publish().channel(channelName).message(messageJsonObject).async(new PNCallback<PNPublishResult>() {
	                    @Override
	                    public void onResponse(PNPublishResult result, PNStatus status) {
	                        // Check whether request successfully completed or not.
	                            if (!status.isError()) {
	 
	                                // Message successfully published to specified channel.
	                        }
	                        // Request processing failed.
	                            else {
	 
	                                // Handle message publish error. Check 'category' property to find out possible issue
	                            // because of which request did fail.
	                            //
	                            // Request can be resent using: [status retry];
	                            }
	                        }
	                    });
	                }
	            }
	            else if (status.getCategory() == PNStatusCategory.PNReconnectedCategory) {
	 
	                // Happens as part of our regular operation. This event happens when
	            // radio / connectivity is lost, then regained.
	            }
	            else if (status.getCategory() == PNStatusCategory.PNDecryptionErrorCategory) {
	 
	                // Handle messsage decryption error. Probably client configured to
	            // encrypt messages and on live data feed it received plain text.
	            }
	        }
	 
	        @Override
	        public void message(PubNub pubnub, PNMessageResult message) {
	            // Handle new message stored in message.message
	        if (message.getChannel() != null) {
	            // Message has been received on channel group stored in
	            // message.getChannel()
	        }
	        else {
	            // Message has been received on channel stored in
	            // message.getSubscription()
	            }
	 
	        
	        //data 
	            JsonElement receivedMessageObject = message.getMessage();
	            System.out.println("Received message content: " + receivedMessageObject.toString());
	        // extract desired parts of the payload, using Gson
	        String msg = message.getMessage().getAsJsonObject().get("timestamp").getAsString();
	  //      PubNubSinkTask();
	          
	    //    System.out.println("msg content: " + msg);
	 
	        PubNubProducer obj = new PubNubProducer();
	        
//	        TwitterKafkaProducer obj = new TwitterKafkaProducer();
	        obj.jobj = receivedMessageObject.toString(); 
	        try {
				obj.run();
			} catch (InterruptedException e) {
				
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        //obj.run(consumerKey, consumerSecret, token, secret);
	       

	            /*
	            log the following items with your favorite logger
	                - message.getMessage()
	                - message.getSubscription()
	                - message.getTimetoken()
	        */
	        }
	 
	        @Override
	        public void presence(PubNub pubnub, PNPresenceEventResult presence) {
	 
	        }	        
	        
	    });
	   
	    pubnub.subscribe().channels(Arrays.asList(channelName)).execute();
	    
	    pubnub.addListener(new SubscribeCallback() {
	        @Override
	        public void status(PubNub pubnub, PNStatus status) {
	            if (status.getOperation() != null) {
	                switch (status.getOperation()) {
	                    // let's combine unsubscribe and subscribe handling for ease of use
	                    case PNSubscribeOperation:
	                    case PNUnsubscribeOperation:
	                        // note: subscribe statuses never have traditional
	                        // errors, they just have categories to represent the
	                        // different issues or successes that occur as part of subscribe
	                        switch (status.getCategory()) {
	                            case PNConnectedCategory:
	                                // this is expected for a subscribe, this means there is no error or issue whatsoever
	                            case PNReconnectedCategory:
	                                // this usually occurs if subscribe temporarily fails but reconnects. This means
	                                // there was an error but there is no longer any issue
	                            case PNDisconnectedCategory:
	                                // this is the expected category for an unsubscribe. This means there
	                                // was no error in unsubscribing from everything
	                            case PNUnexpectedDisconnectCategory:
	                                // this is usually an issue with the internet connection, this is an error, handle appropriately
	                            case PNAccessDeniedCategory:
	                                // this means that PAM does allow this client to subscribe to this
	                                // channel and channel group configuration. This is another explicit error
	                            default:
	                                // More errors can be directly specified by creating explicit cases for other
	                                // error categories of `PNStatusCategory` such as `PNTimeoutCategory` or `PNMalformedFilterExpressionCategory` or `PNDecryptionErrorCategory`
	                        }
	     
	                    case PNHeartbeatOperation:
	                        // heartbeat operations can in fact have errors, so it is important to check first for an error.
	                        // For more information on how to configure heartbeat notifications through the status
	                        // PNObjectEventListener callback, consult <link to the PNCONFIGURATION heartbeart config>
	                        if (status.isError()) {
	                            // There was an error with the heartbeat operation, handle here
	                        } else {
	                            // heartbeat operation was successful
	                        }
	                    default: {
	                        // Encountered unknown status type
	                    }
	                }
	            } else {
	                // After a reconnection see status.getCategory()
	            }
	        }@Override
	        public void message(PubNub pubnub, PNMessageResult message) {
	            String messagePublisher = message.getPublisher();
	            System.out.println("Message publisher: " + messagePublisher);
	            System.out.println("Message Payload: " + message.getMessage());
	            System.out.println("Message Subscription: " + message.getSubscription());
	            System.out.println("Message Channel: " + message.getChannel());
	            System.out.println("Message timetoken: " + message.getTimetoken());
	        }
	     
	        @Override
	        public void presence(PubNub pubnub, PNPresenceEventResult presence) {
	     
	        }
	    });
	     
	    SubscribeCallback subscribeCallback = new SubscribeCallback() {
	        @Override
	        public void status(PubNub pubnub, PNStatus status) {
	     
	        }
	     
	        @Override
	        public void message(PubNub pubnub, PNMessageResult message) {
	     
	        }
	     
	        @Override
	        public void presence(PubNub pubnub, PNPresenceEventResult presence) {
	     
	        }
	    };
	     
	    pubnub.addListener(subscribeCallback);
	     
	    // some time later
	    pubnub.removeListener(subscribeCallback);
	    
	    pubnub.subscribe()
	    .channels(Arrays.asList("my_channel")) // subscribe to channels
	    .execute();
	    
	    
	    JsonObject position = new JsonObject();
	    position.addProperty("lat", 32L);
	    position.addProperty("lng", 32L);
	     
	    System.out.println("before pub: " + position);
	    pubnub.publish()
	        .message(position)
	        .channel("my_channel")
	        .async(new PNCallback<PNPublishResult>() {
	            @Override
	            public void onResponse(PNPublishResult result, PNStatus status) {
	                // handle publish result, status always present, result if successful
	                // status.isError() to see if error happened
	                if(!status.isError()) {
	                    System.out.println("pub timetoken: " + result.getTimetoken());
	                }
	                System.out.println("pub status code: " + status.getStatusCode());
	            }
	        });
	    
	     
	    class complexData {
	        String fieldA;
	        int fieldB;
	    }
	             
	   
	     
	    pubnub.addListener(new SubscribeCallback() {
	        @Override
	        public void status(PubNub pubnub, PNStatus status) {
	            if (status.getCategory() == PNStatusCategory.PNConnectedCategory){
	                complexData data = new complexData();
	                data.fieldA = "Awesome";
	                data.fieldB = 10;
	                pubnub.publish().channel("awesomeChannel").message(data).async(new PNCallback<PNPublishResult>() {
	                    @Override
	                    public void onResponse(PNPublishResult result, PNStatus status) {
	                        // handle publish response
	                    }
	                });
	            }
	        }
	     
	        @Override
	        public void message(PubNub pubnub, PNMessageResult message) {
	     
	        }
	     
	        @Override
	        public void presence(PubNub pubnub, PNPresenceEventResult presence) {
	     
	        }
	    });
	     
	    pubnub.subscribe().channels(Arrays.asList("pubnub-sensor-network"));
	    
	   
	} 
		

	 
}
