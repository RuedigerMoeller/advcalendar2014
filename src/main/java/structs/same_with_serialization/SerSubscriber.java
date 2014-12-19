package structs.same_with_serialization;

import org.nustaq.fastcast.api.FCSubscriber;
import org.nustaq.fastcast.api.FastCast;
import org.nustaq.fastcast.config.SubscriberConf;
import org.nustaq.fastcast.util.RateMeasure;
import org.nustaq.offheap.bytez.Bytez;
import org.nustaq.serialization.simpleapi.OnHeapCoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;


/**
 * Created by ruedi on 19.12.14.
 */
public class SerSubscriber {

    public static void main( String arg[] ) {

        FastCast.getFastCast().setNodeId("SUB"); // 5 chars MAX !!
        SerPublisher.configureFastCast();

        final RateMeasure rateMeasure = new RateMeasure("receive rate");
        byte[] buffer = new byte[4000];
        OnHeapCoder coder = new OnHeapCoder(SerPublisher.SHARED_REFS,SerProtocol.Instrument.class,SerProtocol.PriceUpdate.class);

        ByteArrayInputStream bin = new ByteArrayInputStream(buffer); // for JDK ser test

        FastCast.getFastCast().onTransport("default").subscribe(
            new SubscriberConf(1).receiveBufferPackets(33_000),
            new FCSubscriber() {

                @Override
                public void messageReceived(String sender, long sequence, Bytez b, long off, int len) {
                    b.getArr(off,buffer,0,len);
                    if ( SerPublisher.USEFST ) {
                        SerProtocol.PriceUpdate msg = (SerProtocol.PriceUpdate) coder.toObject(buffer);
                    } else {
                        try {
                            bin.reset();
                            ObjectInputStream in = new ObjectInputStream(bin);
                            if ( SerPublisher.SHARED_REFS ) {
                                SerProtocol.PriceUpdate msg = (SerProtocol.PriceUpdate) in.readObject();
                            } else {
                                SerProtocol.PriceUpdate msg = (SerProtocol.PriceUpdate) in.readUnshared();
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                    rateMeasure.count();
                }

                @Override
                public boolean dropped() {
                    System.out.println("fatal, could not keep up. exiting");
                    System.exit(0);
                    return false;
                }

                @Override
                public void senderTerminated(String senderNodeId) {
                    System.out.println("sender died "+senderNodeId);
                }

                @Override
                public void senderBootstrapped(String receivesFrom, long seqNo) {
                    System.out.println("bootstrap "+receivesFrom);
                }
            }
        );
    }

}
