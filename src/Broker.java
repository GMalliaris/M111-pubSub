import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.stream.Collectors;

//broker -s s_port -p p_port
public class Broker {

    private static final Map<String, Set<String>> topicSubscribers = new HashMap<>();
    private static final Map<String, Socket> subscriberSockets = new HashMap<>();
    private static int subPort;
    private static int pubPort;

    public static void main(String[] args) throws IOException {

//        Runtime.getRuntime().addShutdownHook(new Thread(){public void run(){
//            try {
//                socket.close();
//                System.out.println("The server is shut down!");
//            } catch (IOException e) { /* failed */ }
//        }});

        validateArgs(args);

        try (var pubServerSocket = new ServerSocket(pubPort);
            var subServerSocket = new ServerSocket(subPort)) {
//            1 socket for pubServerSocket.accept and 1 socket for each pub
//            while (true) {
//                var pubSocket = pubServerSocket.accept();
//                readPubCommandAndReply(pubSocket);
//            }
//            while (true) {
//                var subSocket = subServerSocket.accept();
//                readSubCommandAndReply(subSocket);
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void validateArgs(String[] args){
        var invalidArgsMsg = "Invalid arguments";
        var unknownArgTemplate = "Unknown argument: '%s'";
        var validArgFormat = "Broker runs as follows: <broker -s s_port -p p_port>";
        var invalidPort = "Value '%s' is not valid for %s port";

        final var pubPortArg = "-p";
        final var subPortArg = "-s";

        String pubPort = null;
        String subPort = null;

        if (args.length != 4){
            System.err.println(invalidArgsMsg);
            System.err.println(validArgFormat);
            System.exit(-1);
        }

        for (int i = 0; i <= 2; i += 2){
            if (subPortArg.equals(args[i])) {
                subPort = args[i + 1];
            }
            else if (pubPortArg.equals(args[i])) {
                pubPort = args[i + 1];
            }
            else {
                System.err.println(invalidArgsMsg);
                System.err.println(String.format(unknownArgTemplate, args[i]));
                System.err.println(validArgFormat);
                System.exit(-1);
            }
        }

        if (pubPort == null || subPort == null){
            System.err.println(invalidArgsMsg);
            System.err.println(validArgFormat);
            System.exit(-1);
        }

        try {
            Broker.subPort = Integer.parseInt(subPort);
        }
        catch (NumberFormatException e){
            System.err.println(invalidArgsMsg);
            System.err.println(String.format(invalidPort, subPort, "subscribers'"));
            System.exit(-1);
        }

        try {
            Broker.pubPort = Integer.parseInt(pubPort);
        }
        catch (NumberFormatException e){
            System.err.println(invalidArgsMsg);
            System.err.println(String.format(invalidPort, pubPort, "publishers'"));
            System.exit(-1);
        }
    }

    static synchronized void synchronizedLog(String message) {
        System.out.println(message);
    }

    private static void readPubCommandAndReply(Socket pubSocket){
        try (var pubOutStream = new PrintWriter(pubSocket.getOutputStream(), true);
             var pubInStream = new BufferedReader(new InputStreamReader(pubSocket.getInputStream()))  ){

            var inputLine = pubInStream.readLine();
            while (inputLine != null) {
                Thread.sleep(1000);
                // send msg to subs
                var split = inputLine.split(" ", 4);
                sendMessageToTopic(split[3], split[2]);
                pubOutStream.println("OK");
                inputLine = pubInStream.readLine();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private static void readSubCommandAndReply(Socket subSocket){
        try (var subOutStream = new PrintWriter(subSocket.getOutputStream(), true);
             var subInStream = new BufferedReader(new InputStreamReader(subSocket.getInputStream()))  ){

            var inputLine = subInStream.readLine();
            while (inputLine != null) {
                Thread.sleep(1000);
                var split = inputLine.split(" ", 3);
                synchronized (subscriberSockets){
                    if (subscriberSockets.get(split[0]) == null) {
                        subscriberSockets.put(split[0], subSocket);
                    }
                }
                if ("sub".equals(split[1])){
                    subscribeToTopic(split[0], split[2]);
                }
                else {
                    unsubscribeFromTopic(split[0], split[2]);
                }
                subOutStream.println("OK");
                inputLine = subInStream.readLine();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private static void subscribeToTopic(String subId, String topic) {
        synchronized (topicSubscribers){
            var topicSubscribers = Broker.topicSubscribers.getOrDefault(topic, new HashSet<>());
            topicSubscribers.add(subId);
            Broker.topicSubscribers.put(topic, topicSubscribers);
        }
    }

    private static void unsubscribeFromTopic(String subId, String topic) {
        synchronized (topicSubscribers){
            var topicSubscribers = Broker.topicSubscribers.get(topic);
            topicSubscribers.remove(subId);
            Broker.topicSubscribers.put(topic, topicSubscribers);
        }
    }

    private static void sendMessageToTopic(String message, String topic){

        List<Socket> subsSockets;
        synchronized (topicSubscribers){
            synchronized (subscriberSockets){
                subsSockets = topicSubscribers.getOrDefault(topic, Set.of())
                        .stream()
                        .map(subscriberSockets::get)
                        .collect(Collectors.toList());
            }
        }
        subsSockets.forEach(subSocket -> {
            try (var clientOutStream = new PrintWriter(subSocket.getOutputStream(), true)){
                clientOutStream.println(String.format("%s %s", topic, message));
            } catch (IOException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        });
    }
}
