import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Scanner;
import java.util.Set;
import java.util.Iterator;
import java.util.Arrays;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.PrintStream;

import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static java.lang.System.out;

class NS {
    // Range for this NS is (rangeLower...rangeUpper]
    final int rangeUpper; // id
    int rangeLower;
    // Address I bind to.
    InetSocketAddress myAddr;
    // upstream address. (lower range).
    InetSocketAddress prevAddr;
    // Downstream address (higher range).
    InetSocketAddress nextAddr;

    // Invarient: nextAddr rangeLower=this.rangeUpper

    // Address of BNS.
    InetSocketAddress bsAddr;
    Object dataMutex;
    SortedMap<Integer, String> data;
    ServerSocket ss;

    NS(int id, int port, InetSocketAddress bsAddr_) throws IOException {
        rangeUpper = id;
        bsAddr = bsAddr_;
        data = new TreeMap<>();
        dataMutex = new Object();
        // Make the assumption that we connect to all other
        // servers through the same local interface.
        try (Socket s = new Socket()) {
            s.connect(bsAddr);
            InetAddress a = s.getLocalAddress();
            myAddr = new InetSocketAddress(a, port);
        }
        ss = new ServerSocket(port);
        out.printf("NS id %d at %s\n", id, myAddr.toString());
        ss.setReuseAddress(true);
        (new Thread(()->networkRun())).start();
        (new Thread(()->userRun())).start();

    }

    private void doEnter() throws IOException {
        prevAddr = bsAddr;
        nextAddr = bsAddr;
        boolean locationFound = false;
        out.printf("Entering ring. id %d\n", rangeUpper);
        while (!locationFound) {
            try (Socket s = new Socket()) {
                out.println("querying " + nextAddr);
                s.connect(nextAddr);
                s.getOutputStream().write("query\n".getBytes());
                Scanner sin = new Scanner(s.getInputStream());
                int upRL = sin.nextInt();
                int upRU = sin.nextInt();
                String nextHost = sin.next();
                int port = sin.nextInt();
                prevAddr = nextAddr;
                nextAddr = new InetSocketAddress(InetAddress.getByName(nextHost),
                                                 port);
                out.printf("query response: range (%s %d] nextAddr %s\n",
                           upRL, upRU, nextAddr.toString());
                if (upRU < rangeUpper && upRU > rangeUpper) {
                    locationFound = true;
                    rangeLower = upRL;
                    out.printf("My location found. Range (%d %d]\n" +
                               "prevAddr %s, nextAddr %s\n",
                               rangeLower, rangeUpper,
                               prevAddr.toString(),
                               nextAddr.toString());
                }
            }
        }
        // Send enterprev
        try (Socket s = new Socket()) {
            out.println("Sending enterprev to " + prevAddr);
            s.connect(prevAddr);
            s.getOutputStream().write(String.format("enterprev %s %d\n",
                                                    myAddr.getHostString(),
                                                    myAddr.getPort()).getBytes());
            (new Scanner(s.getInputStream())).nextLine();
        }
        // Send enternext
        try (Socket s = new Socket()) {
            out.println("Sending enternext to %s" + nextAddr);
            s.connect(nextAddr);
            s.getOutputStream().write(String.format("enternext %d %s %d\n",
                                                    rangeUpper,
                                                    myAddr.getHostString(),
                                                    myAddr.getPort()).getBytes());
            (new Scanner(s.getInputStream())).nextLine(); // check ok?
        }
    }

    private void doExit() throws IOException {
        try (Socket s = new Socket()) {
            s.connect(prevAddr);
            s.getOutputStream().write(String.format("exitprev %d %s %d\n",
                                                    prevAddr.getHostString(),
                                                    prevAddr.getPort()).getBytes());
            (new Scanner(s.getInputStream())).nextLine();// check ok?
            rangeLower = rangeUpper;
        }
        try (Socket s = new Socket()) {
            s.connect(nextAddr);
            s.getOutputStream().write(String.format("exitnext %d %s %d\n",
                                                    rangeLower,
                                                    nextAddr.getHostString(),
                                                    nextAddr.getPort()).getBytes());
            (new Scanner(s.getInputStream())).nextLine();// check ok?
        }
        xferData(prevAddr);
    }

    private void userRun(){
        try{
            Scanner sin = new Scanner(System.in);

            while (sin.hasNextLine()) {
                String[] cmd = sin.nextLine().trim().split("\\s+");
                switch (cmd[0].toLowerCase()) {
                case "enter":
                    doEnter();
                    break;
                case "exit":
                    doExit();
                    break;
                default:
                    System.out.println("Unknown command");
                }
            }
        }catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    private void xferData(InetSocketAddress dest) throws IOException {
        synchronized (dataMutex) {
            for (int key: data.keySet()) {
                if (key > rangeLower && key <= rangeUpper) {
                    continue;
                }
                String msg = data.get(key);
                data.remove(key);
                out.printf("xfering key %d to %s\n", key, dest.toString());
                try (Socket s = new Socket()) {
                    s.connect(dest);
                    s.getOutputStream().write(String.
                                              format("insert %d %s\n", key, msg).getBytes());
                    String response = (new Scanner(s.getInputStream())).nextLine().trim();
                    if (!(response.split("\\s+")[0].toLowerCase().equals("ok"))) {
                        throw new IllegalStateException("Zone transfer failure of " + key);
                    }
                }
            }
        }
    }
    private void processNetworkCommand(Socket s) throws IOException, UnknownHostException {
        String line = (new Scanner(s.getInputStream())).nextLine().trim();

        String [] cmd = line.split("\\s+");
        PrintStream ps = new PrintStream(s.getOutputStream());
        switch (cmd[0].toLowerCase()) {
        case "query":
        {
            out.printf("Received query. range (%d %d], next %s\n",
                       rangeLower, rangeUpper, nextAddr.toString());
            ps.printf("%d %d %s %d\n", rangeLower, rangeUpper,
                      nextAddr.getHostString(),
                      nextAddr.getPort());
        }
        break;
        case "enterprev":
        {
            nextAddr = new InetSocketAddress(InetAddress.getByName(cmd[1]),
                                             Integer.parseInt(cmd[2]));
            out.println("receive enterprev. New nextAddr " + nextAddr);
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
            out.printf("received enternext. new range (%d, %d]. prev %s\n",
                       rangeLower, rangeUpper, prevAddr.toString());
            ps.print("ok\n");
            xferData(prevAddr);
        }
        break;
        case "exitnext":
        {
            rangeLower = Integer.parseInt(cmd[1]);
            if (rangeLower >= rangeUpper) {
                throw new IllegalStateException();
            }
            prevAddr = new InetSocketAddress(InetAddress.getByName(cmd[2]),
                                             Integer.parseInt(cmd[3]));
            out.printf("Received exitnext. new range (%d %d], prevAddr %s\n",
                       rangeLower, rangeUpper, prevAddr);
            ps.print("ok\n");
        }
        break;
        case "exitprev":
        {
            nextAddr = new InetSocketAddress(InetAddress.getByName(cmd[1]),
                                             Integer.parseInt(cmd[2]));
            out.println("Receive exitprev. nextAddr " + nextAddr);
            ps.print("ok\n");
        }
        break;
        case "lookup":
        {
            synchronized (dataMutex) {
                int key = Integer.parseInt(cmd[1]);
                out.printf("received lookup for key %d\n");
                if (key <= rangeLower || key > rangeUpper) {
                    out.println("Responding no nextAddr " + nextAddr);
                    ps.printf("no %s %d", nextAddr.getHostString(),
                              nextAddr.getPort());
                }else if (data.get(key) == null) {
                    out.println("responding na");
                    ps.printf("na\n");
                }else{
                    out.printf("responding ok %s\n", data.get(key));
                    ps.printf("ok %s\n", data.get(key));
                }
            }
        }
        break;
        case "insert":
        {
            int key = Integer.parseInt(cmd[1]);
            out.printf("Received insert for key %d\n", key);
            if (key <= rangeLower || key > rangeUpper) {
                out.printf("Responding no %s" + nextAddr);
                ps.printf("no %s %d\n",
                          nextAddr.getHostString(), nextAddr.getPort());
            }else{
                String msg = String.join(" ", Arrays.copyOfRange(cmd, 2, cmd.length));
                synchronized (dataMutex) {
                    data.put(key, msg);
                }
                ps.print("ok\n");
            }
        }
        break;
        case "delete":
        {
            int key = Integer.parseInt(cmd[1]);
            out.println("Received delete for key " + key);
            if (key <= rangeLower || key > rangeUpper) {
                out.println("Responding no  next " + nextAddr);
                ps.printf("no %s %d\n",
                          nextAddr.getHostString(), nextAddr.getPort());
            }else{
                synchronized (dataMutex) {
                    if (data.get(key) == null) {
                        out.println("Can't find key " + key);
                        ps.printf("na\n");
                    }else{
                        out.println("Removing key " + key);
                        ps.printf("ok\n");
                        data.remove(key);
                    }
                }
            }
        }
        break;
        default:
            throw new IllegalArgumentException("Unknown command: " + cmd[0]);
        }
    }

    private void networkRun(){
        while (true) {
            try (Socket s = ss.accept()) {
                processNetworkCommand(s);
            }catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public static void main(String [] args) throws FileNotFoundException, IOException {
        try (Scanner sin = new Scanner(new File(args[0]))) {
            int id = sin.nextInt();
            int myPort = sin.nextInt();
            InetSocketAddress bsAddr = new InetSocketAddress(sin.next(), sin.nextInt());
            new NS(id, myPort, bsAddr);
        }
    }
}
