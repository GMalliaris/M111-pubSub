import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

//subscriber -i ID -r sub_port -h broker_IP -p port [-f command_file]
public class Subscriber {

    private static String id;
    private static int port;
    private static String brokerIp;
    private static int brokerPort;
    private static String commandFile;
    private static Socket brokerSocket;
    private static boolean shutDown = false;
    private static String pendingCommand = null;

    private static void sendExitCommand(){
        if (brokerSocket == null || brokerSocket.isClosed()){
            return;
        }
        try {
            var socketOutStream = new PrintWriter(brokerSocket.getOutputStream(), true);
            socketOutStream.println(String.format("%s exit", id));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Runnable gracefulShutdownRunnable() {
        return () -> {

            sendExitCommand();
            shutDown = true;
            if (brokerSocket != null && !brokerSocket.isClosed()){
                try {
                    brokerSocket.close();
                } catch (IOException e) {
                    System.err.println("Failed to close broker socket");
                }
            }
            System.out.println("Closed broker socket");
        };
    }

    public static void main(String[] args) {
        validateArgs(args);

        Runtime.getRuntime().addShutdownHook(new Thread(gracefulShutdownRunnable()));

        List<String> commands = commandFile == null ? List.of()
                : readCommandsFromFile(commandFile);
        try (var cmdScanner = new Scanner(System.in)){
            brokerSocket = new Socket(brokerIp, brokerPort, InetAddress.getLocalHost(), port);
            sendCommandsFromFileToBroker(commands, brokerSocket);

            var readOnPortThread = new Thread(readOnPortRunnable());
            readOnPortThread.start();

            System.out.printf("Subscriber with id: '%s' is up!%n", id);

            System.out.println("\nPlease enter a command in the following format: <SUB_ID COMMAND TOPIC>");
            var userInput = cmdScanner.nextLine();
            while (userInput != null){
                if (commandIsValid(userInput)){
                    sendCommand(userInput);
                }
                System.out.println("\nPlease enter a command in the following format: <SUB_ID COMMAND TOPIC>");
                userInput = cmdScanner.nextLine();
            }
        } catch (IOException e) {
            if (!shutDown){
                e.printStackTrace();
            }
        }
    }

    private static Runnable readOnPortRunnable(){
        return () -> {
            try {
                var socketInStream = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()));
                var message = socketInStream.readLine();
                while (message != null){
                    var split = message.split(" ", 2);
                    if ("OK".equals(split[0])){
                        var completedCommand = removePendingCommand();
                        var cmdSplit = completedCommand.split(" ", 3);
                        if ("sub".equals(cmdSplit[1])){
                            System.out.println(String.format("Subscribed to topic: %s", cmdSplit[2]));
                        }
                        else{
                            System.out.println(String.format("Unsubscribed from topic: %s", cmdSplit[2]));
                        }
                    }
                    else{
                        System.out.printf("Received msg for topic %s: %s%n", split[0], split[1]);
                    }
                    message = socketInStream.readLine();
                }
            } catch (IOException e) {
                if (!shutDown){
                    e.printStackTrace();
                }
            }
        };
    }

    private static void sendCommandsFromFileToBroker(List<String> commands, Socket brokerSocket) {
        commands.stream()
                .map(Subscriber::parseCommand)
                .filter(element -> Subscriber.commandIsValid(element[1]))
                .forEach(element -> {
                    try {
                        var waitInterval = Integer.parseInt(element[0]) * 1000; // in milliseconds
                        Thread.sleep(waitInterval);
                        sendCommandAndWaitForOK(element[1]);

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        Thread.currentThread().interrupt();
                    } catch (NumberFormatException e){
                        System.err.printf("Invalid wait interval: '%s'%n", element[0]);
                    }
                });
    }

    private static void sendCommand(String command) {
        if (!addPendingCommand(command)) {
            return;
        }
        try {
            var socketOutStream = new PrintWriter(brokerSocket.getOutputStream(), true);
            socketOutStream.println(command);
        } catch (IOException e) {
            if (!shutDown){
                e.printStackTrace();
            }
        }
    }

    private static void sendCommandAndWaitForOK(String command) {
        try {
            var socketOutStream = new PrintWriter(brokerSocket.getOutputStream(), true);
            var socketInStream = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()));
            socketOutStream.println(command);
            var response = socketInStream.readLine();
            while (!"OK".equals(response)){
                if (response == null){
                    System.err.println("Couldn't connect to broker");
                }
                else {
                    var split = response.split(" ", 2);
                    System.out.printf("Received msg for topic %s: %s%n", split[0], split[1]);
                }
                response = socketInStream.readLine();
            }
            var split = command.split(" ", 3);
            if ("sub".equals(split[1])){
                System.out.println(String.format("Subscribed to topic: %s", split[2]));
            }
            else {
                System.out.println(String.format("Unsubscribed from topic: %s", split[2]));
            }
        } catch (IOException e) {
            if (!shutDown){
                e.printStackTrace();
            }
        }
    }

    private static boolean commandIsValid(String userInput) {

        var split = userInput.split(" ", 3);

        var isValid = true;
        if (split.length != 3){
            System.err.println("Invalid command format");
            isValid = false;
        }
        if (!id.equals(split[0])){
            System.err.printf("Invalid id: '%s'%n", split[0]);
            isValid = false;
        }
        if (!"sub".equals(split[1]) && !"unsub".equals(split[1])){
            System.err.printf("Invalid command: '%s'%n", split[1]);
            isValid = false;
        }
        return isValid;
    }

    private static String[] parseCommand(String command){
        var split = command.split(" ", 2);
        return new String[]{split[0], String.format("%s %s", id, split[1])};
    }

    private static List<String> readCommandsFromFile(String commandFile) {

        var commands = new LinkedList<String>();
        var file = new File(commandFile);
        try(var scanner = new Scanner(file)) {
            while (scanner.hasNextLine()){
                var command = scanner.nextLine();
                commands.add(command);
            }

        } catch (FileNotFoundException e) {
            System.err.println("Command file is invalid");
            System.exit(-1);
        }
        return commands;
    }

    private static void validateArgs(String[] args){
        var invalidArgsMsg = "Invalid arguments";
        var unknownArgTemplate = "Unknown argument: '%s'";
        var validArgFormat = "Subscriber runs as follows: <Subscriber -i ID -r sub_port -h broker_IP -p port [-f command_file]>";
        var invalidPort = "Value '%s' is not valid for %s port";

        var idArg = "-i";
        var portArg = "-r";
        var brokerIpArg = "-h";
        var brokerPortArg = "-p";
        var commandFileArg = "-f";

        String id = null;
        String port = null;
        String brokerIp = null;
        String brokerPort = null;
        String commandFile = null;

        if (args.length != 8 && args.length != 10){
            System.err.println(invalidArgsMsg);
            System.err.println(validArgFormat);
            System.exit(-1);
        }

        for (int i = 0; i < args.length; i += 2){
            if (idArg.equals(args[i])){
                id = args[i + 1];
            }
            else if (portArg.equals(args[i])){
                port = args[i + 1];
            }
            else if (brokerIpArg.equals(args[i])){
                brokerIp = args[i + 1];
            }
            else if (brokerPortArg.equals(args[i])){
                brokerPort = args[i + 1];
            }
            else if (commandFileArg.equals(args[i])){
                commandFile = args[i + 1];
            }
            else {
                System.err.println(invalidArgsMsg);
                System.err.println(String.format(unknownArgTemplate, args[i]));
                System.err.println(validArgFormat);
                System.exit(-1);
            }
        }

        if (id == null || port == null || brokerIp == null || brokerPort == null ){
            System.err.println(invalidArgsMsg);
            System.err.println(validArgFormat);
            System.exit(-1);
        }

        try {
            Subscriber.port = Integer.parseInt(port);
        }
        catch(NumberFormatException e){
            System.err.println(invalidArgsMsg);
            System.err.println(String.format(invalidPort, port, "subscriber"));
            System.exit(-1);
        }

        try {
            Subscriber.brokerPort = Integer.parseInt(brokerPort);
        }
        catch(NumberFormatException e){
            System.err.println(invalidArgsMsg);
            System.err.println(String.format(invalidPort, brokerPort, "broker"));
            System.exit(-1);
        }

        Subscriber.id = id;
        Subscriber.brokerIp = brokerIp;
        Subscriber.commandFile = commandFile;
    }

    private static synchronized boolean addPendingCommand(String command){
        if (pendingCommand == null){
            pendingCommand = command;
            return true;
        }
        else {
            System.err.println(String.format("There is already a pending command: '%s'", pendingCommand));
        }
        return false;
    }

    private static synchronized String removePendingCommand(){
        var command = pendingCommand;
        pendingCommand = null;
        return command;
    }
}
