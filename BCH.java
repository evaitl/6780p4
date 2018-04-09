import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Scanner;
import java.util.Set;
import java.util.Iterator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;

import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.ClosedChannelException;

import java.net.StandardSocketOptions;
import java.net.InetSocketAddress;

class BCH {
    Selector selector;
    Object dataMutex;
    SortedMap<Integer, String> data;

    BCH(int port, SortedMap<Integer, String> data_) throws IOException {
        data = data_;
        dataMutex = new Object();
        selector = Selector.open();
        ServerSocketChannel ssc = ServerSocketChannel.open();
        ssc.bind(new InetSocketAddress(port));
        ssc.configureBlocking(false);
        ssc.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        ssc.register(selector, SelectionKey.OP_ACCEPT, null);
        (new Thread(()->networkRun())).start();
        (new Thread(()->userRun())).start();
    }

    private void insert(int key, String value){
    }

    private void delete(int key){
    }

    private String lookup(int key){
        return null;
    }

    private void userRun(){
        Scanner sin = new Scanner(System.in);

        while (sin.hasNextLine()) {
            String[] cmd = sin.nextLine().trim().split("\\s+");
            switch (cmd[0].toLowerCase()) {
            case "lookup":
            case "insert":
            case "delete":
            default:
            }
        }
    }

    private void networkRun(){
        try{
            while (true) {
                int numKeys = selector.select();
                Set<SelectionKey>selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iter = selectedKeys.iterator();
                while (iter.hasNext()) {
                    SelectionKey ky = iter.next();
                    if (ky.isAcceptable()) {
                        SocketChannel sc = ((ServerSocketChannel)ky.channel()).accept();
                        sc.configureBlocking(false);
                        sc.register(selector, SelectionKey.OP_READ);
                    }else if (ky.isReadable()) {
                        processNetworkCommand((SocketChannel)ky.channel());
                    }
                }
            }
        }catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void processNetworkCommand(SocketChannel sc){
        // sc.read(), sc.write(), sc.close()
    }

    public static void main(String [] args) throws FileNotFoundException, IOException {
        try (Scanner sin = new Scanner(new File(args[0]))) {
            sin.nextInt();
            int port = sin.nextInt();
            SortedMap<Integer, String> data = new TreeMap<>();
            while (sin.hasNextInt()) {
                data.put(sin.nextInt(), sin.next());
            }
            new BCH(port, data);
        }
    }
}
