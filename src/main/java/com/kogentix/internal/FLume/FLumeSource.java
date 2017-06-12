package com.kogentix.internal.FLume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;

public class FLumeSource extends AbstractSource implements Configurable, PollableSource {

  @Override public void configure(Context context) {
    // TODO
  }

  @Override public Status process() throws EventDeliveryException {
    Status status = Status.READY;

    String msg = "";
    Event event = EventBuilder.withBody(msg.getBytes());

    getChannelProcessor().processEvent(event);

    return status;

  }
}