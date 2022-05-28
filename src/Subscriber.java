import java.io.File;
import java.io.FileNotFoundException;
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

    public static void main(String[] args) {
        validateArgs(args);

        List<String> commands = commandFile == null ? List.of()
                : readCommandsFromFile(commandFile);
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
}
