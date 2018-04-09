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


class CH {
    // Range for this CH is (rangeLower...rangeUpper]
    final int rangeUpper;
    int rangeLower;
    // Address I bind to.
    InetSocketAddress myAddr;
    // upstream address. (lower range).
    InetSocketAddress prevAddr;
    // Downstream address (higher range).
    InetSocketAddress nextAddr;

    // Invarient: nextAddr rangeLower=this.rangeUpper

    // Address of BCH.
    InetSocketAddress bsAddr;
    Object dataMutex;
    SortedMap<Integer, String> data;
    ServerSocket ss;

    CH(int id_, int port, InetSocketAddress bsAddr_) throws IOException {
        rangeUpper = id_;
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
        ss.setReuseAddress(true);
        (new Thread(()->networkRun())).start();
        (new Thread(()->userRun())).start();

    }

    private void doEnter() throws IOException {
        prevAddr = bsAddr;
        nextAddr = bsAddr;
        boolean locationFound = false;
        while (!locationFound) {
            try (Socket s = new Socket()) {
                s.connect(prevAddr);
                s.getOutputStream().write("query\n".getBytes());
                Scanner sin = new Scanner(s.getInputStream());
                int upRL = sin.nextInt();
                int upRH = sin.nextInt();
                String nextHost = sin.next();
                int port = sin.nextInt();
                if (upRL < rangeUpper && upRH > rangeUpper) {
                    locationFound = true;
                    rangeLower = upRL;
                }else{
                    nextAddr = prevAddr;
                    prevAddr = new InetSocketAddress(InetAddress.getByName(nextHost),
                                                     port);
                }
            }
        }
        // Send enterdown
        try (Socket s = new Socket()) {
            s.connect(nextAddr);
            s.getOutputStream().write(String.format("enterdown %s %d\n",
                                                    myAddr.getHostString(),
                                                    myAddr.getPort()).getBytes());
            (new Scanner(s.getInputStream())).nextLine();
        }
        // Send enterup
        try (Socket s = new Socket()) {
            s.connect(prevAddr);
            s.getOutputStream().write(String.format("enterup %d %s %d\n",
                                                    rangeUpper,
                                                    myAddr.getHostString(),
                                                    myAddr.getPort()).getBytes());
            (new Scanner(s.getInputStream())).nextLine(); // check ok?
        }
    }

    private void doExit() throws IOException {
        try (Socket s = new Socket()) {
            s.connect(prevAddr);
            s.getOutputStream().write(String.format("exitup %d %s %d\n",
                                                    rangeLower,
                                                    prevAddr.getHostString(),
                                                    prevAddr.getPort()).getBytes());
            (new Scanner(s.getInputStream())).nextLine();// check ok?
            rangeLower = rangeUpper;
        }
        try (Socket s = new Socket()) {
            s.connect(nextAddr);
            s.getOutputStream().write(String.format("exitdown %s %d\n",
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
            ps.printf("%d %s %d\n", rangeUpper,
                      nextAddr.getHostString(),
                      nextAddr.getPort());
        }
        break;
        case "enterdown":
        {
            prevAddr = new InetSocketAddress(InetAddress.getByName(cmd[1]),
                                             Integer.parseInt(cmd[2]));
            ps.print("ok\n");
        }
        break;
        case "enterup":
        {
            rangeLower = Integer.parseInt(cmd[1]);
            if (rangeLower >= rangeUpper) {
                throw new IllegalStateException();
            }
            nextAddr = new InetSocketAddress(InetAddress.getByName(cmd[2]),
                                             Integer.parseInt(cmd[3]));
            ps.print("ok\n");
            xferData(nextAddr);
        }
        break;
        case "exitup":
        {
            rangeLower = Integer.parseInt(cmd[1]);
            if (rangeLower >= rangeUpper) {
                throw new IllegalStateException();
            }
            nextAddr = new InetSocketAddress(InetAddress.getByName(cmd[2]),
                                             Integer.parseInt(cmd[3]));
            ps.print("ok\n");
        }
        break;
        case "exitdown":
        {
            prevAddr = new InetSocketAddress(InetAddress.getByName(cmd[1]),
                                             Integer.parseInt(cmd[2]));
            ps.print("ok\n");
        }
        break;
        case "lookup":
        {
            synchronized (dataMutex) {
                int key = Integer.parseInt(cmd[1]);
                if (key <= rangeLower || key > rangeUpper) {
                    ps.printf("no %s %d\n", prevAddr.getHostString(), prevAddr.getPort());
                }else if (data.get(key) == null) {
                    ps.printf("na\n");
                }else{
                    ps.printf("ok %s\n", data.get(key));
                }
            }
        }
        break;
        case "insert":
        {
            int key = Integer.parseInt(cmd[1]);
            if (key <= rangeLower || key > rangeUpper) {
                ps.printf("no %s %d\n",
                          prevAddr.getHostString(), prevAddr.getPort());
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
            if (key <= rangeLower || key > rangeUpper) {
                ps.printf("no %s %d\n",
                          prevAddr.getHostString(), prevAddr.getPort());
            }else{
                synchronized (dataMutex) {
                    data.remove(Integer.parseInt(cmd[1]));
                }
                ps.print("ok\n");
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
            new CH(id, myPort, bsAddr);
        }
    }
}
