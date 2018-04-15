import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Scanner;
import java.util.Set;
import java.util.Iterator;
import java.util.Arrays;
import java.util.NoSuchElementException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.PrintStream;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.lang.System.out;
class BNS {
    final int rangeUpper;
    int rangeLower;
    ServerSocket ss;
    InetSocketAddress myAddr;
    InetSocketAddress nextAddr;
    InetSocketAddress prevAddr;
    Object dataMutex;
    SortedMap<Integer, String> data;

    static class QueryResponse {
        QueryResponse(int rangeLower_, int rangeUpper_,
                      InetSocketAddress server_, InetSocketAddress next_){
            rangeLower = rangeLower_;
            rangeUpper = rangeUpper_;
            next = next_;
            server = server_;
        }
        final int rangeLower;
        final int rangeUpper;
        final InetSocketAddress server;
        final InetSocketAddress next;
    }

    BNS(int port, SortedMap<Integer, String> data_) throws IOException {
        data = data_;
        dataMutex = new Object();
        // IMHO the spec is broken because I can't set up a ring without
        // knowing which IP address to use for the BNS. Slime our way around this
        // with getLocalHost().

        // see: https://goo.gl/3NQZJ1
        myAddr = new InetSocketAddress(InetAddress.getLocalHost(), port);
        nextAddr = myAddr;
        prevAddr = myAddr;
        ss = new ServerSocket(port);
        rangeLower = -1;
        rangeUpper = 1024;
        out.printf("BNS key 0 on port %d nextAddr %s, prevAddr %s, range (%d, %d]\n",
                   port,
                   nextAddr.toString(), prevAddr.toString(),
                   rangeLower, rangeUpper);
        (new Thread(()->networkRun())).start();
        (new Thread(()->userRun())).start();
    }

    private void insert(int key, String value) throws IOException {
        if (key < 0 || key > 1023) {
            throw new IllegalArgumentException("Key out of range: " + key);
        }
        if (key > rangeLower && key <= rangeUpper) {
            synchronized (dataMutex) {
                out.println("Inserted in BNS");
                data.put(key, value);
            }
        }else{
            boolean serverFound = false;
            InetSocketAddress next = nextAddr;
            QueryResponse qr = null;
            while (!serverFound) {
                qr = query(next);
                out.printf("Tried server %d\n", qr.rangeUpper);
                if (key > qr.rangeLower && key <= qr.rangeUpper) {
                    serverFound = true;
                    out.println("Success");
                }
                next = qr.next;
            }
            try (Socket s = new Socket()) {
                s.connect(next);
                String msg = String.format("insert %d %s\n",
                                           key, value);
                s.getOutputStream().write(msg.getBytes());
                String resp = (new Scanner(s.getInputStream())).
                              nextLine().trim().split("\\s+")[0];
                if (!resp.equals("ok")) {
                    throw new IllegalStateException("insertion failed: " + key);
                }
            }
        }
    }

    private void delete(int key) throws IOException {
        if (key < 0 || key > 1023) {
            throw new IllegalArgumentException("Key out of range: " + key);
        }
        if (key > rangeLower && key <= rangeUpper) {
            synchronized (dataMutex) {
                out.println("Deleting from BNS");
                if (data.get(key) == null) {
                    out.println("key not found");
                }else{
                    data.remove(key);
                    out.println("successful deletion");
                }
            }
        }else{
            boolean serverFound = false;
            InetSocketAddress next = nextAddr;
            QueryResponse qr = null;
            while (!serverFound) {
                qr = query(next);
                if (key > qr.rangeLower && key <= qr.rangeUpper) {
                    serverFound = true;
                    out.println("Success");
                }else{
                    next = qr.next;
                }
            }
            try (Socket s = new Socket()) {
                s.connect(next);
                String msg = String.format("delete %d\n",
                                           key);
                s.getOutputStream().write(msg.getBytes());
                String resp = (new Scanner(s.getInputStream())).
                              nextLine().trim().split("\\s+")[0];
                if (resp.equals("ok")) {
                    out.println("successful deletion");
                }else if (resp.equals("na")) {
                    out.println("key not found");
                }else{
                    throw new IllegalStateException("insertion failed: " + key);
                }
            }
        }
    }

    private String lookup(InetSocketAddress isa, int key) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(isa);
            s.getOutputStream().write(String.format("lookup %d\n", key).getBytes());
            String resp = (new Scanner(s.getInputStream())).nextLine();
            if (resp.trim().split("\\s+")[0].equals("ok")) {
                return resp.trim().substring(3);
            }
        }
        return null;
    }

    private void lookup(int key) throws IOException {
        if (key < 0 || key > 1023) {
            throw new IllegalArgumentException("key out of range: " + key);
        }
        if (key == 0 || (key > rangeLower && key <= rangeUpper)) {
            synchronized (dataMutex) {
                if (data.get(key) != null) {
                    out.printf("key %d msg: %s\n", key, data.get(key));
                }else{
                    out.println("Key not found");
                }
            }
        }else{
            boolean serverFound = false;
            QueryResponse qr = null;
            InetSocketAddress qAddr = nextAddr;
            while (!serverFound) {
                qr = query(qAddr);
                out.printf("server %d tried\n", qr.rangeUpper);
                if (key > qr.rangeLower && key <= qr.rangeUpper) {
                    serverFound = true;
                }else{
                    qAddr = qr.next;
                }
            }
            String lu = lookup(qAddr, key);
            if (lu == null) {
                out.println("key not found");
            }else{
                out.println("msg: " + lu);
            }
        }
    }

    private QueryResponse query(InetSocketAddress isa) throws IOException {
        try (Socket s = new Socket()) {
            s.connect(isa);
            s.getOutputStream().write("query\n".getBytes());
            Scanner sin = new Scanner(s.getInputStream());
            int rl = sin.nextInt();
            int ru = sin.nextInt();
            String nextAddr = sin.next();
            int nextPort = sin.nextInt();
            return new QueryResponse(rl, ru, isa,
                                     new InetSocketAddress(InetAddress.
                                                           getByName(nextAddr),
                                                           nextPort));
        }
    }

    private void userRun(){
        Scanner sin = new Scanner(System.in);

        while (sin.hasNextLine()) {
            try{
                String[] cmd = sin.nextLine().trim().split("\\s+");
                switch (cmd[0].toLowerCase()) {
                case "lookup":
                    lookup(Integer.parseInt(cmd[1]));
                    break;
                case "insert":
                    insert(Integer.parseInt(cmd[1]),
                           String.join(" ", Arrays.copyOfRange(cmd, 2, cmd.length)));
                    break;
                case "delete":
                    delete(Integer.parseInt(cmd[1]));
                    break;
                default:
                    System.out.println("Unknown command: " + cmd[0]);
                }
            }catch (IOException e) {
                System.out.println("Exception caught: " + e);
            }
        }
    }

    private void networkRun(){
        while (true) {
            try (Socket s = ss.accept()) {
                processNetworkCommand(s);
            }catch (IOException e) {
                System.out.println("Networking exception: " + e);
            }
        }
    }
    private void xferData(InetSocketAddress dest) throws IOException {
        synchronized (dataMutex) {
            TreeSet<Integer> keySet = new TreeSet<>(data.keySet());
            for (int key: keySet) {
                if (key == 0 || (key > rangeLower && key <= rangeUpper)) {
                    continue;
                }
                if (!data.containsKey(key)) {
                    out.println("Huh... Doesn't contain key " + key);
                    continue;
                }
                String msg = data.get(key);
                data.remove(key);
                out.printf("xfer out  %d to %s %d\n",
                           key, dest.getHostString(), dest.getPort());
                try (Socket s = new Socket()) {
                    s.connect(dest);
                    s.getOutputStream().
                    write(String.
                          format("insert %d %s\n", key, msg).
                          getBytes());
                    String response = (new Scanner(s.getInputStream())).nextLine().trim();
                    if (!(response.split("\\s+")[0].equals("ok"))) {
                        throw new IllegalStateException("Zone transfer failure of " + key);
                    }
                }
            }
        }
    }
    private void processNetworkCommand(Socket s) throws IOException,
    UnknownHostException {
        String line = null;

        try{
            line = (new Scanner(s.getInputStream())).nextLine().trim();
        }catch (NoSuchElementException e) {
            return;
        }
        String [] cmd = line.split("\\s+");
        PrintStream ps = new PrintStream(s.getOutputStream());
        switch (cmd[0].toLowerCase()) {
        case "query":
        {
            int rl = (rangeLower == -1 ? 0 : rangeLower);
            ps.printf("%d %d %s %d\n", rl, rangeUpper,
                      nextAddr.getHostString(),
                      nextAddr.getPort());
            out.printf("Recived query. Responding (%d, %d] nextAddr %s\n",
                       rangeLower, rangeUpper, nextAddr.toString());

        }
        break;
        case "enterprev":
        {
            nextAddr = new InetSocketAddress(InetAddress.getByName(cmd[1]),
                                             Integer.parseInt(cmd[2]));
            out.println("Received enterprev. New nextAddr: " +
                        nextAddr);
            ps.print("ok\n");
        }
        break;
        case "enternext":
        {
            rangeLower = Integer.parseInt(cmd[1]);
            if (rangeLower >= rangeUpper) {
                throw new IllegalStateException();
            }
            prevAddr = new InetSocketAddress(InetAddress.getByName(cmd[2]),
                                             Integer.parseInt(cmd[3]));
            out.printf("Received enternext. prevAddr %s New range (%d, %d])\n",
                       prevAddr.toString(), rangeLower, rangeUpper);
            ps.print("ok\n");

            xferData(prevAddr);
        }
        break;
        case "exitnext":
        {
            rangeLower = Integer.parseInt(cmd[1]);
            if (rangeLower == 0) {
                rangeLower = -1;
            }
            if (rangeLower >= rangeUpper) {
                throw new IllegalStateException();
            }
            prevAddr = new InetSocketAddress(InetAddress.getByName(cmd[2]),
                                             Integer.parseInt(cmd[3]));
            out.printf("recieved exitNext. new prevAddr %s. New range (%d 0]\n",
                       prevAddr.toString(),
                       rangeLower);
            ps.print("ok\n");
        }
        break;
        case "exitprev":
        {
            nextAddr = new InetSocketAddress(InetAddress.getByName(cmd[1]),
                                             Integer.parseInt(cmd[2]));
            out.println("received exitprev. New nextAddr %s" + nextAddr);
            ps.print("ok\n");
        }
        break;
        case "lookup":
        {
            synchronized (dataMutex) {
                int key = Integer.parseInt(cmd[1]);
                out.printf("received lookup for key %d\n");
                if (key <= rangeLower || key > rangeUpper) {
                    ps.printf("no %s %d\n", nextAddr.getHostString(), nextAddr.getPort());
                    out.printf("responded: no %s\n",
                               nextAddr.toString());
                }else if (data.get(key) == null) {
                    ps.printf("na\n");
                    out.printf("responded: na\n");
                }else{
                    ps.printf("ok %s\n", data.get(key));
                    out.printf("responded: %s\n", data.get(key));
                }
            }
        }
        break;
        case "insert":
        {
            int key = Integer.parseInt(cmd[1]);
            out.printf("insert request for %d\n", key);
            if (key <= rangeLower || key > rangeUpper) {
                ps.printf("no %s %d\n",
                          nextAddr.getHostString(), nextAddr.getPort());
                out.printf("responded: no %s %d\n",
                           nextAddr.getHostString(), nextAddr.getPort());
            }else{
                String msg = String.join(" ", Arrays.copyOfRange(cmd, 2, cmd.length));
                synchronized (dataMutex) {
                    out.printf("Inserting %d:%s\n", key, msg);
                    data.put(key, msg);
                }
                ps.print("ok\n");
            }
        }
        break;
        case "delete":
        {
            int key = Integer.parseInt(cmd[1]);
            out.printf("delete request for %d\n", key);
            if (key <= rangeLower || key > rangeUpper) {
                ps.printf("no %s %d\n",
                          nextAddr.getHostString(), nextAddr.getPort());
                out.printf("responded: no %s %d\n", nextAddr.getHostString(),
                           nextAddr.getPort());
            }else{
                out.printf("removing key %d\n", key);
                synchronized (dataMutex) {
                    data.remove(key);
                }
                ps.print("ok\n");
            }
        }
        break;
        default:
            throw new IllegalArgumentException("Unknown command: " + cmd[0]);
        }
    }

    public static void main(String [] args) throws FileNotFoundException, IOException {
        try (Scanner sin = new Scanner(new File(args[0]))) {
            sin.nextInt();
            int port = sin.nextInt();
            SortedMap<Integer, String> data = new TreeMap<>();
            while (sin.hasNextInt()) {
                data.put(sin.nextInt(), sin.next());
            }
            new BNS(port, data);
        }
    }
}
