package structs.same_with_serialization;

import org.nustaq.serialization.simpleapi.OnHeapCoder;

import java.io.Serializable;

/**
 * Created by ruedi on 19.12.14.
 */
public class SerProtocol {

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

    public static void do20Millions(byte[] networkBuffer, PriceUpdate msg) {
        OnHeapCoder coder = new OnHeapCoder(false,Instrument.class,PriceUpdate.class);
        long tim = System.currentTimeMillis();
        int size = 0;
        for ( int i = 0; i < 20_000_000; i++ ) {

            Instrument instrument = msg.getInstrument();
            instrument.setMnemonic("BMW");
            instrument.setInstrumentId(13);
            msg.setPrc(99.0);
            msg.setQty(100);

            size = coder.toByteArray( msg, networkBuffer, 0, networkBuffer.length );
        }
        System.out.println("tim: " + (System.currentTimeMillis() - tim)+" "+size);
    }

    public static void main(String s[]) {

        PriceUpdate template = new PriceUpdate();
        // demonstrates that theoretical send rate is >20 millions messages per second on
        // an I7 box
        byte networkBuffer[] = new byte[4000];
        PriceUpdate msg = new PriceUpdate();
        while ( true ) {
            do20Millions(networkBuffer, msg);
        }

//        System.out.println(msg);
//        System.out.println("size:" + msg.getByteSize());

    }


}
