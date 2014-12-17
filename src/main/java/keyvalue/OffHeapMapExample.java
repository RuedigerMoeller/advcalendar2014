package keyvalue;

import org.nustaq.offheap.FSTAsciiStringOffheapMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by ruedi on 15.12.14.
 *
 * Run with -Xmx12g -Xms12g, else vm will not able to run the OnHeap regular HashMap test
 *
 *
 */
public class OffHeapMapExample {

    FSTAsciiStringOffheapMap<User> memory;
    HashMap<String,User> hugeMap;

    public OffHeapMapExample() throws Exception {
        hugeMap = new HashMap<>(15_000_000);
    }

    public void initMap() {
        // pure memory variant
        // memory = new FSTAsciiStringOffheapMap<>(20, 4*FSTAsciiStringOffheapMap.GB, 500_000);
        try {
            memory = new FSTAsciiStringOffheapMap<>("/tmp/storeadvent.mmf", 20, 4*FSTAsciiStringOffheapMap.GB, 500_000);
            memory.getCoder().getConf().registerClass(User.class); // avoid writing full classnames
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void fifteenMillion(FSTAsciiStringOffheapMap<User> memory) {
        int size = memory.getSize();
        if ( size < 15_000_000 ) {
            for ( int i = 0; i < 15_000_000; i++ ) {
                User newUser = new User("u"+i, (i%1 == 0) ? "Adam "+i : "Eva "+i, "username"+i, System.currentTimeMillis() );
                newUser.addFriend("u"+(i-1)).addFriend("u"+(i+1));
                memory.put(newUser.getUid(), newUser);
            }
        }
        System.out.println(memory.getCapacityMB()-memory.getFreeMem()/1024/1024);
    }

    public void fifteenMillionOnHeap() {
        for ( int i = 0; i < 15_000_000; i++ ) {
            User newUser = new User("u"+i, (i%1 == 0) ? "Adam "+i : "Eva "+i, "username"+i, System.currentTimeMillis() );
            newUser.addFriend("u"+(i-1)).addFriend("u"+(i+1));
            hugeMap.put( newUser.getUid(), newUser);
        }
    }

    public void doSomeLookupsOnHeap() {
        for ( int i = 0; i < 100_000; i++ ) {
            String key = "u" + (int) (Math.random() * 100000);
            User user = hugeMap.get(key);
            if ( ! key.equals(user.getUid()) ) {
                throw new RuntimeException("this is very bad");
            }
        }
    }

    public void doSomeLookups() {
        for ( int i = 0; i < 100_000; i++ ) {
            String key = "u" + (int) (Math.random() * 100000);
            User user = memory.get(key);
            if ( ! key.equals(user.getUid()) ) {
                throw new RuntimeException("this is very bad");
            }
        }
    }

    public long measureTime( String title, Runnable r ) {
        long tim = System.currentTimeMillis();
        r.run();
        long duration = System.currentTimeMillis() - tim;
        System.out.println("duration of '" + title + "' was " + duration);
        return duration;
    }

    private void measureGC() {
        long tim = System.currentTimeMillis();
        System.gc();
        System.out.println("duration of GC "+(System.currentTimeMillis() - tim));
        System.out.println("~ Heap Size " + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())/1024/1024+"MB");
    }

    public static void main(String arg[]) throws Exception {
        OffHeapMapExample ex = new OffHeapMapExample();
        ex.measureTime("create map offheap", ex::initMap);

        ex.measureTime("fill map offheap", () -> ex.fifteenMillion(ex.memory));

        for ( int i = 0; i < 20; i++ ) {
            long dur = ex.measureTime("100_000 accesses", () -> ex.doSomeLookups());
            System.out.println((1000l*100000)/dur+" accesses per second");
        }
        ex.measureGC();

        ex.measureTime( "create map ON heap", () -> ex.fifteenMillionOnHeap() );
        for ( int i = 0; i < 20; i++ ) {
            long dur = ex.measureTime("100_000 accesses", () -> ex.doSomeLookupsOnHeap());
            System.out.println((1000l*100000l)/dur+" accesses per second");
        }
        ex.measureGC();


    }

    public static class User implements Serializable {
        String uid;

        String firstName;
        String name;

        long lastLogin;
        ArrayList<String> friends = new ArrayList<>();

        public User(String uid, String firstName, String name, long lastLogin) {
            this.uid = uid;
            this.firstName = firstName;
            this.name = name;
            this.lastLogin = lastLogin;
        }

        public String getUid() {
            return uid;
        }

        public void setUid(String uid) {
            this.uid = uid;
        }

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getLastLogin() {
            return lastLogin;
        }

        public void setLastLogin(long lastLogin) {
            this.lastLogin = lastLogin;
        }

        public ArrayList<String> getFriends() {
            return friends;
        }

        public void setFriends(ArrayList<String> friends) {
            this.friends = friends;
        }

        public User addFriend( String s ) {
            friends.add(s);
            return this;
        }

        public User addFriend( User p ) {
            friends.add(p.getUid());
            return this;
        }

        @Override
        public String toString() {
            return "Person{" +
                       "uid='" + uid + '\'' +
                       ", firstName='" + firstName + '\'' +
                       ", name='" + name + '\'' +
                       ", lastLogin=" + lastLogin +
                       ", friends=" + friends +
                       '}';
        }
    }


}
