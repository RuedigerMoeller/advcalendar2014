package keyvalue;

import org.nustaq.kontraktor.*;
import org.nustaq.kontraktor.Callback;
import org.nustaq.kontraktor.impl.ElasticScheduler;
import org.nustaq.kontraktor.remoting.tcp.TCPActorClient;
import org.nustaq.offheap.FSTAsciiStringOffheapMap;

import javax.security.auth.callback.*;

import static keyvalue.OffHeapMapExample.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by ruedi on 15.12.2014.
 */
public class KVClient {

    public static void main(String arg[]) throws IOException {
        ElasticScheduler.DEBUG_SCHEDULING = false; // kontraktor beta is chatty ..

        Future<KVServer> connect = TCPActorClient.Connect(KVServer.class, "127.0.0.1", 7777)
            .onResult( server -> {
                try {
                    // warmup
                    benchGet(server, true);

                    // filter some stuff remotely using a simple spore ...
                    server.$iterateValues(new Spore<User, Object>() {
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

                    server.$sync(); // one ping pong to ensure all messages have been processed

                    String initUser = "u13";

                    // compute social graph fetching data asynchronous
                    int depth = 5;

                    System.out.println("friends level " + depth + " of " + initUser + ":");
                    ArrayList<User> socialGraph = new ArrayList<User>();
                    socialGraph(server, Arrays.asList(initUser), depth, socialGraph)
                        .onResult(signal -> socialGraph.stream().distinct().forEach(System.out::println));

                    server.$sync();

                    // compute social graph using spores (better)
                    socialGraphWithSpores(server, initUser, depth).onResult(list -> {
                        System.out.println("social graph from spore:");
                        list.forEach(System.out::println);
                        System.out.println("--");
                    });

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

    private static Future<List<User>> socialGraphWithSpores(KVServer server, final String initUser, final int depth) {
        Promise p = new Promise();
        List<User> resSocialGraph = new ArrayList<>();

        server.$onMap(
            new Spore<FSTAsciiStringOffheapMap, Object>() {
                // capture context
                String start = initUser;
                int d = depth;

                @Override
                public void remote(FSTAsciiStringOffheapMap map) {
                    HashSet<String> visited = new HashSet<String>();
                    visited.add(initUser);
                    socialGraph(map, (User) map.get(initUser), visited, depth);
                    receive(null, Callback.FIN);
                    finished();
                }
                private void socialGraph(FSTAsciiStringOffheapMap map, User u, HashSet<String> visited, int dep) {
                    if (dep > 0) {
                        u.getFriends().forEach(userId -> {
                            if (!visited.contains(userId)) {
                                visited.add(userId);
                                User user = (User) map.get(userId);
                                receive(user, Callback.CONT);
                                socialGraph(map, user, visited, dep - 1);
                            }
                        });
                    }
                }
            }.then( (result, error) -> {
                // handle results coming in from remote
                if ( ! Callback.FIN.equals(error) ) {
    //                    System.out.println("spore result:" + result);
                    resSocialGraph.add((User) result);
                } else {
                    p.receive(resSocialGraph,null); // fulfill promise with full list
                }
            })
        );
        return p;
    }

    // inefficient as some users are queried twice (see map()) .. check spore above is better (move code not data)
    public static Future socialGraph( KVServer server, List<String> friends, int depth, List<User> result ) {
        if ( depth > 0 ) {
            Promise fin = new Promise();
            Actors.yield( friends.stream().map( friend -> server.$get(friend) ).toArray(Future[]::new))
                  .onResult(futures -> {
                      ArrayList<Future> friendQueries = new ArrayList<>();
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
