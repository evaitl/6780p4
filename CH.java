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


class CH{
    int rangeUpper;
    int rangeLower;
    InetSocketAddress myAddr;
    InetSocketAddress upAddr;
    InetSocketAddress downAddr;
    InetSocketAddress bsAddr;    
    SortedMap<Integer,String> data;
    ServerSocket ss;
    
    CH(int id_, int port, InetSocketAddress bsAddr_) throws IOException {
	rangeUpper=id_;	
	bsAddr=bsAddr_;	
	data=new TreeMap<>();

	// Make the assumption that we connect to all other
	// servers through the same local interface. 
	try(Socket s=new Socket()){
	    s.connect(bsAddr);
	    InetAddress a=s.getLocalAddress();
	    myAddr=new InetSocketAddress(a,port);
	}
	ss=new ServerSocket(port);
	ss.setReuseAddress(true);
	(new Thread(()->networkRun())).start();
	(new Thread(()->userRun())).start();
	
    }

    private void doEnter(){
	
    }
    
    private void doExit(){
    }
    
    private void userRun(){
	Scanner sin=new Scanner(System.in);
	while(sin.hasNextLine()){
	    String[] cmd=sin.nextLine().trim().split("\\s+");
	    switch(cmd[0].toLowerCase()){
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
    }
    private void xferData(InetSocketAddress dest) throws IOException{
	for(int key: data.keySet()){
	    if(key>rangeLower && key<=rangeUpper){
		continue;
	    }
	    String msg=data.get(key);
	    data.remove(key);
	    try(Socket s=new Socket(dest.getAddress(),dest.getPort())){
		s.getOutputStream().write(String.
					  format("insert %d %s\n",key,msg).getBytes());
		String response=(new Scanner(s.getInputStream())).nextLine().trim();
		if(!(response.split("\\s+")[0].toLowerCase().equals("ok"))){
		    throw new IllegalStateException("Zone transfer failure of "+key);
		}
	    }
	}
    }
    private void processNetworkCommand(Socket s) throws IOException, UnknownHostException {
	String line=(new Scanner(s.getInputStream())).nextLine().trim();
	String []cmd=line.split("\\s+");
	PrintStream ps=new PrintStream(s.getOutputStream());
	switch(cmd[0].toLowerCase()){
	case "query":
	    {
		ps.printf("%d %s %d\n",rangeUpper,
			  upAddr.getHostString(),
			  upAddr.getPort());
	    }
	    break;
	case "enterdown":
	    {
		int upId=Integer.parseInt(cmd[1]);
		int upPort=Integer.parseInt(cmd[3]);
		upAddr=new InetSocketAddress(InetAddress.getByName(cmd[2]),upPort);
		ps.print("ok\n");
	    }
	    break;
	case "enterup":
	    {
		rangeLower=Integer.parseInt(cmd[1]);
		if(rangeLower>=rangeUpper){
		    throw new IllegalStateException();
		}
		downAddr=new InetSocketAddress(InetAddress.getByName(cmd[2]),
					       Integer.parseInt(cmd[3]));
		xferData(downAddr);
		ps.print("ok\n");
	    }
	    break;
	case "exitup":
	    {
		rangeLower=Integer.parseInt(cmd[1]);
		if(rangeLower>=rangeUpper){
		    throw new IllegalStateException();
		}
		downAddr=new InetSocketAddress(InetAddress.getByName(cmd[2]),
					       Integer.parseInt(cmd[3]));
		ps.print("ok\n");
	    }
	    break;
	case "exitdown":
	    {
		upAddr=new InetSocketAddress(InetAddress.getByName(cmd[1]),
					     Integer.parseInt(cmd[2]));
		ps.print("ok\n");
	    }
	    break;
	case "lookup":
	    {
		int key=Integer.parseInt(cmd[1]);
		if(key <= rangeLower || key >rangeUpper){
		    ps.printf("no %s %d\n",upAddr.getHostString(),upAddr.getPort());
		}else if(data.get(key)==null){
		    ps.printf("na\n");
		}else{
		    ps.printf("ok %s\n",data.get(key));
		}
	    }
	break;
	case "insert":
	    {
		int key=Integer.parseInt(cmd[1]);
		if(key<=rangeLower || key>rangeUpper){
		    ps.printf("no %s %d\n",
			      upAddr.getHostString(),upAddr.getPort());
		}else{
		    String msg=String.join(" ",Arrays.copyOfRange(cmd,2,cmd.length));
		    data.put(key,msg);
		    ps.print("ok\n");
		}
	    }
	    break;
	case "delete":
	    {
		int key=Integer.parseInt(cmd[1]);
		if(key <= rangeLower || key>rangeUpper){
		    ps.printf("no %s %d\n",
			      upAddr.getHostString(),upAddr.getPort());
		}else{
		    data.remove(Integer.parseInt(cmd[1]));
		    ps.print("ok\n");
		}
	    }
	    break;
	default:
	    throw new IllegalArgumentException("Unknown command: "+cmd[0]);
	}
    }
    
    private void networkRun(){
	while(true){
	    try(Socket s=ss.accept()){
		processNetworkCommand(s);
	    }catch(IOException e){
		throw new UncheckedIOException(e);
	    }
	}
    }
    
    public static void main(String [] args) throws FileNotFoundException, IOException{
	try(Scanner sin=new Scanner(new File(args[0]))){
	    int id =sin.nextInt();
	    int myPort=sin.nextInt();
	    InetSocketAddress bsAddr=new InetSocketAddress(sin.next(), sin.nextInt());
	    new CH(id,myPort, bsAddr);
	}
    }
}
