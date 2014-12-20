package structs.same_with_serialization;

import org.nustaq.fastcast.api.FCPublisher;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.PhysicalTransportConf;
import org.nustaq.fastcast.config.PublisherConf;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.serialization.simpleapi.OnHeapCoder;

import static structs.same_with_serialization.SerProtocol.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by ruedi on 19.12.14.
 */
public class SerPublisher {

    public static void main(String arg[]) throws IOException {

        FastCast.getFastCast().setNodeId("PUB"); // 5 chars MAX !!
        configureFastCast();

        FCPublisher pub = FastCast.getFastCast().onTransport("default").publish(
                new PublisherConf(1)            // unique-per-transport topic id
                    .numPacketHistory(33_000)   // how many packets are kept for retransmission requests
                    // NOTE: for slower CPU's/network stacks reduce this to 500 .. 2000
                    // in case receiver gets dropped
                    .pps(5_000)                // packets per second rate limit.
        );

        PriceUpdate msg = new PriceUpdate();

        OnHeapCoder coder = new OnHeapCoder(SHARED_REFS,Instrument.class,PriceUpdate.class);
        byte codingBuffer[] = new byte[4000];

        ThreadLocalRandom current = ThreadLocalRandom.current();

        ByteArrayOutputStream baos = new ByteArrayOutputStream(4000);

        // could directly send raw on publisher
        RateMeasure measure = new RateMeasure("msg/s");
        while( true ) {
            measure.count();

            // fill in data
            Instrument instrument = msg.getInstrument();
            instrument.setMnemonic("BMW");
            instrument.setInstrumentId(13);
            msg.setPrc(99.0+current.nextDouble(10.0)-5);
            msg.setQty(100+current.nextInt(10));

            if ( USEFST ) {
                int len = coder.toByteArray(msg,codingBuffer,0,4000);
                // send message
                while( ! pub.offer(null,codingBuffer,0,len,false) ) {
                    /* spin */
                }
            } else { // use JDK serialization
                baos.reset();
                ObjectOutputStream oos = new ObjectOutputStream(baos); // cannot reuse
                if ( SHARED_REFS )
                    oos.writeObject(msg);
                else
                    oos.writeUnshared(msg);
                oos.close();
                byte[] bytes = baos.toByteArray(); // must copy + allocate
                // send message
                while( ! pub.offer(null,bytes,0,bytes.length,false) ) {
                    /* spin */
                }
            }

        }
    }

    public static void configureFastCast() {
        // note this configuration is far below possible limits regarding throughput and rate
        FastCast fc = FastCast.getFastCast();
        fc.addTransport(
                new PhysicalTransportConf("default")
                        .interfaceAdr("127.0.0.1")  // define the interface
                        .port(42055)                // port is more important than address as some OS only test for ports ('crosstalking')
                        .mulitcastAdr("229.9.9.17")  // ip4 multicast address
                        .setDgramsize(64_000)         // datagram size. Small sizes => lower latency, large sizes => better throughput [range 1200 to 64_000 bytes]
                        .socketReceiveBufferSize(4_000_000) // as large as possible .. however avoid hitting system limits in example
                        .socketSendBufferSize(2_000_000)
                        // uncomment this to enable spin looping. Will increase throughput once datagram size is lowered below 8kb or so
//                        .idleParkMicros(1)
//                        .spinLoopMicros(100_000)
        );

    }

}
