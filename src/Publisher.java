import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

//publisher -i ID -r pub_port -h broker_IP -p broker_port [-f command_file]
public class Publisher {

    private static String id;
    private static int port;
    private static String brokerIp;
    private static int brokerPort;
    private static String commandFile;
    private static Socket brokerSocket;
    private static boolean shutDown = false;

    private static Runnable gracefulShutdownRunnable() {
        return () -> {
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
            sendCommandsFromFileToBroker(commands);

            System.out.println("\nPlease enter a command in the following format: <PUB_ID COMMAND TOPIC MESSAGE>");
            var userInput = cmdScanner.nextLine();
            while (userInput != null){
                if (commandIsValid(userInput)){
                    sendCommandAndWaitForOK(userInput);
                }
                System.out.println("\nPlease enter a command in the following format: <PUB_ID COMMAND TOPIC MESSAGE>");
                userInput = cmdScanner.nextLine();
            }
        } catch (IOException e) {
            if (!shutDown){
                e.printStackTrace();
            }
        }
    }

    private static boolean commandIsValid(String userInput) {

        var split = userInput.split(" ", 4);
        if (split.length != 4 || !id.equals(split[0]) || !"pub".equals(split[1])){
            System.err.println("Invalid command");
            return false;
        }
        return true;
    }

    private static void sendCommandsFromFileToBroker(List<String> commands) {
        commands.stream()
                .map(Publisher::parseCommand)
                .filter(element -> Publisher.commandIsValid(element[1]))
                .forEach(element -> {
                    try {
                        var waitInterval = Integer.parseInt(element[0]) * 1000; // in milliseconds
                        Thread.sleep(waitInterval);
                        sendCommandAndWaitForOK(element[1]);

                    } catch (InterruptedException e) {
                        if (!shutDown){
                            e.printStackTrace();
                        }
                        Thread.currentThread().interrupt();
                    } catch (NumberFormatException e){
                        System.err.printf("Invalid wait interval: '%s'%n", element[0]);
                    }
                });
    }

    private static String[] parseCommand(String command){
        var split = command.split(" ", 2);
        return new String[]{split[0], String.format("%s %s", id, split[1])};
    }

    private static List<String> readCommandsFromFile(String commandFile) {

        var commands = new LinkedList<String>();
        var file = new File(commandFile);
        try(var scanner = new Scanner(file);) {
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

    public static void sendCommandAndWaitForOK(String command){
        try {
            var socketOutStream = new PrintWriter(brokerSocket.getOutputStream(), true);
            var socketInStream = new BufferedReader(new InputStreamReader(brokerSocket.getInputStream()));
            socketOutStream.println(command);
            var response = socketInStream.readLine();
            if (response == null){
                System.err.println("Couldn't connect to broker");
            }
            else if (!"OK".equals(response)){
                System.err.println(String.format("Got response '%s' for command '%s'", response, command));
            }
            else {
                var split = command.split(" ", 4);
                System.out.println(String.format("Published message for topic %s: %s", split[2], split[3]));
            }
        } catch (IOException e) {
            if (!shutDown){
                e.printStackTrace();
            }
        }
    }

    private static void validateArgs(String[] args){
        var invalidArgsMsg = "Invalid arguments";
        var unknownArgTemplate = "Unknown argument: '%s'";
        var validArgFormat = "Publisher runs as follows: <publisher -i ID -r pub_port -h broker_IP -p port [-f command_file]>";
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
            Publisher.port = Integer.parseInt(port);
        }
        catch(NumberFormatException e){
            System.err.println(invalidArgsMsg);
            System.err.println(String.format(invalidPort, port, "publisher"));
            System.exit(-1);
        }

        try {
            Publisher.brokerPort = Integer.parseInt(brokerPort);
        }
        catch(NumberFormatException e){
            System.err.println(invalidArgsMsg);
            System.err.println(String.format(invalidPort, brokerPort, "broker"));
            System.exit(-1);
        }

        Publisher.id = id;
        Publisher.brokerIp = brokerIp;
        Publisher.commandFile = commandFile;
    }
}
