package structs.same_with_serialization;

import org.nustaq.serialization.simpleapi.OnHeapCoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Created by ruedi on 19.12.14.
 */
public class SerProtocol {

    public static final boolean SHARED_REFS = true;
    public static final boolean USEFST = true;

    public static class Instrument implements Serializable {

        String mnemonic = "";
        long instrumentId;

        public String getMnemonic() {
            return mnemonic;
        }

        public void setMnemonic(String mnemonic) {
            this.mnemonic = mnemonic;
        }

        public long getInstrumentId() {
        return instrumentId;
    }

        public void setInstrumentId(long instrumentId) {
        this.instrumentId = instrumentId;
    }

        @Override
        public String toString() {
            return "Instrument{" +
                       "mnemonic='" + mnemonic + '\'' +
                       ", instrumentId=" + instrumentId +
                       '}';
        }
    }

    public static class PriceUpdate implements Serializable {

        protected Instrument instrument = new Instrument();
        protected int qty = 0;
        protected double prc;

        public Instrument getInstrument() {
            return instrument;
        }

        public void setInstrument(Instrument instrument) {
            this.instrument = instrument;
        }

        public int getQty() {
            return qty;
        }

        public void setQty(int qty) {
            this.qty = qty;
        }

        public double getPrc() {
            return prc;
        }

        public void setPrc(double prc) {
            this.prc = prc;
        }

        @Override
        public String toString() {
            return "PriceUpdateStruct{" +
                       "instrument=" + instrument +
                       ", qty=" + qty +
                       ", prc=" + prc +
                       '}';
        }
    }
    
    public static void do20MillionsJDK(byte[] networkBuffer, PriceUpdate msg) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4000);
        long tim = System.currentTimeMillis();
        int size = 0;
        int iterations = 20_000_000;
        for ( int i = 0; i < iterations; i++ ) {

            Instrument instrument = msg.getInstrument();
            instrument.setMnemonic("BMW");
            instrument.setInstrumentId(13);
            msg.setPrc(99.0);
            msg.setQty(100);

            baos.reset();
            ObjectOutputStream oos = new ObjectOutputStream(baos); // cannot reuse
            if ( SHARED_REFS )
                oos.writeObject(msg);
            else
                oos.writeUnshared(msg);
            oos.close();
            
            byte[] bytes = baos.toByteArray(); // must copy + allocate
            size = bytes.length;
        }
        long dur = System.currentTimeMillis() - tim;
        System.out.println("tim: "+ dur +" "+(iterations/dur)*1000+" per second. size "+size);
    }

    public static void do20Millions(byte[] networkBuffer, PriceUpdate msg) {
        OnHeapCoder coder = new OnHeapCoder(SHARED_REFS,Instrument.class,PriceUpdate.class);
        long tim = System.currentTimeMillis();
        int size = 0;
        int iterations = 20_000_000;
        for ( int i = 0; i < iterations; i++ ) {

            Instrument instrument = msg.getInstrument();
            instrument.setMnemonic("BMW");
            instrument.setInstrumentId(13);
            msg.setPrc(99.0);
            msg.setQty(100);

            size = coder.toByteArray( msg, networkBuffer, 0, networkBuffer.length );
        }
        long dur = System.currentTimeMillis() - tim;
        System.out.println("tim: "+ dur +" "+(iterations/dur)*1000+" per second. size "+size);
    }

    public static void main(String s[]) throws IOException {

        PriceUpdate template = new PriceUpdate();
        // demonstrates that theoretical send rate is >20 millions messages per second on
        // an I7 box
        byte networkBuffer[] = new byte[4000];
        PriceUpdate msg = new PriceUpdate();
        while ( true ) {
            if ( USEFST )
                do20Millions(networkBuffer, msg);
            else
                do20MillionsJDK(networkBuffer,msg);
        }

//        System.out.println(msg);
//        System.out.println("size:" + msg.getByteSize());

    }


}
