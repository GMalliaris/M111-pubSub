import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

//broker -s s_port -p p_port
public class Broker {

    private static Map<String, Set<String>> topicSubscribers = new HashMap<>();
    private static Map<String, Socket> subscriberSockets = new HashMap<>();
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
            while (true) {
                var pubSocket = pubServerSocket.accept();

                readPubCommandAndReply(pubSocket);
            }
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

    private static void readPubCommandAndReply(Socket clientSocket){
        try (var clientOutStream = new PrintWriter(clientSocket.getOutputStream(), true);
             var clientInStream = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))  ){

            var inputLine = clientInStream.readLine();
            while (inputLine != null) {
                Thread.sleep(1000);
                // send msg to subs
                clientOutStream.println("OK");
                inputLine = clientInStream.readLine();
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}
