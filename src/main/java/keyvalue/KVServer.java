package keyvalue;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.remoting.http.rest.RestActorServer;
import org.nustaq.kontraktor.remoting.tcp.TCPActorServer;
import org.nustaq.offheap.FSTAsciiStringOffheapMap;
import keyvalue.OffHeapMapExample.*;

import java.io.IOException;

/**
 * Created by ruedi on 15.12.14.
 */
public class KVServer extends Actor {
    FSTAsciiStringOffheapMap<Person> memory;

    public Future $init() {
        try {
            memory = new FSTAsciiStringOffheapMap<>("/tmp/storeadvent.mmf", 20, 4*FSTAsciiStringOffheapMap.GB, 500_000);
            memory.getCoder().getConf().registerClass(Person.class); // avoid writing full classnames
            System.out.println("server intialized");
        } catch (Exception e) {
            return new Promise<>(null,e);
        }
        return new Promise<>("yes");
    }

    public Future $get( String uid ) {
        return new Promise<>( memory.get(uid) );
    }

    public Future<Person> $put( String uid, OffHeapMapExample.Person value ) {
        return new Promise<>( memory.get(uid) );
    }

    public void iterateValues( Spore<Person,Person> spore ) {
        memory.values().forEachRemaining( person -> spore.remote(person) );
        spore.finished();
    }

    public Future<Integer> getSize() {
        return new Promise<>(memory.getSize());
    }

    public static void main(String arg[]) throws IOException {
        KVServer servingActor = Actors.AsActor(KVServer.class);
        servingActor.$init().onResult(result -> {
            try {
                TCPActorServer.Publish(servingActor, 7777);
                // http://localhost:8088/kvservice/$get/u13
                RestActorServer.Publish("kvservice", 8088, servingActor);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).onError(error -> ((Exception) error).printStackTrace());
    }
}
