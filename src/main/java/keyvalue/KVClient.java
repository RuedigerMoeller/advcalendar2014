package keyvalue;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.impl.ElasticScheduler;
import org.nustaq.kontraktor.remoting.tcp.TCPActorClient;

import static keyvalue.OffHeapMapExample.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by ruedi on 15.12.2014.
 */
public class KVClient {

    public static void main(String arg[]) throws IOException {
        ElasticScheduler.DEBUG_SCHEDULING = false; // kontraktor beta is chatty ..

        Future<KVServer> connect = TCPActorClient.Connect(KVServer.class, "127.0.0.1", 7777)
            .onResult(server -> {
                try {
                    // warmup
                    benchGet(server, true);

                    // filter some stuff remotely ...
                    server.iterateValues(new Spore<User, Object>() {
                        transient int count = 0;

                        @Override
                        public void remote(User input) {
                            if (input.getUid().startsWith("u1")) {
                                count++;
                                if (count > 10) {
                                    finished();
                                } else {
                                    receive(input, Callback.CONT);
                                }
                            }
                        }

                        @Override
                        public void local(Object result, Object error) {
                            System.out.println("received:" + result);
                        }
                    });

                    server.$sync();

                    // compute social graph example
                    System.out.println("friends level 4 of 'u13':");
                    ArrayList<User> socialGraph = new ArrayList<User>();
                    socialGraph( server, Arrays.asList("u13"), 4, socialGraph )
                        .onResult( signal -> socialGraph.stream().distinct().forEach(System.out::println) );

                    server.$sync();

                    // test throughput
                    while (true) {
                        benchGet(server, true);
                        benchGet(server, false);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            })
            .onError(error -> System.out.println("connection error " + error));
    }

    // inefficient as some users are queried twice (see map()) .. just a base example
    public static Future socialGraph( KVServer server, List<String> friends, int depth, List<User> result ) {
        if ( depth > 0 ) {
            Promise fin = new Promise();
            Actors.yield( friends.stream().map( friend -> server.$get(friend) ).toArray(Future[]::new))
                  .onResult(futures -> {
                      ArrayList<Future> friendQueries = new ArrayList<Future>();
                      for (int i = 0; i < futures.length; i++) {
                          User friendUser = (User) futures[i].getResult();
                          if (!result.contains(friendUser)) {
                              result.add(friendUser);
                              friendQueries.add(socialGraph(server, friendUser.getFriends(), depth - 1, result));
                          }
                      }
                      Actors.yield(friendQueries).onResult(signal -> fin.signal());
                });
            return fin;
        }
        return new Promise<>("done");
    }

    private static void benchGet(KVServer server, boolean existing) throws InterruptedException {
        final int numMsg = 100_000;
        CountDownLatch latch = new CountDownLatch(numMsg);
        long tim = System.currentTimeMillis();

        for ( int i = 0; i < numMsg; i++ ) {
            if ( existing ) {
                server.$get("u" + latch.getCount()).then((person, err) -> {
                    latch.countDown();
                });
            } else {
                server.$get("norecord").then((person, err) -> {
                    latch.countDown();
                });
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long dur = System.currentTimeMillis()-tim;
        System.out.print("dur 100k messages (ms): " + dur);
        System.out.println(" rate get ("+(existing?"existing":"null result")+") per second:"+(numMsg*1000l/dur));
    }
}
